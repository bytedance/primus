/*
 * Copyright 2022 Bytedance Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.primus.runtime.kubernetesnative.am.scheduler;

import static com.bytedance.primus.am.container.ContainerManagerEventType.FORCIBLY_SHUTDOWN;
import static com.bytedance.primus.am.container.ContainerManagerEventType.GRACEFUL_SHUTDOWN;
import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.container.ContainerManager;
import com.bytedance.primus.am.container.ContainerManagerEvent;
import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerContainerCompletedEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEventType;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.ApplicationId;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;
import com.bytedance.primus.runtime.kubernetesnative.am.PodLauncher;
import com.bytedance.primus.runtime.kubernetesnative.am.PodLauncherResult;
import com.bytedance.primus.runtime.kubernetesnative.common.meta.KubernetesDriverMeta;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.KubernetesPodStarter;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.ResourceNameBuilder;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KubernetesContainerManager extends ContainerManager {

  // TODO: remove this workaround
  public static final ApplicationAttemptId FAKE_YARN_APPLICATION_ID = ApplicationAttemptId
      .newInstance(ApplicationId.fromString("application_000_000"), 0 /* AttemptId */);

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesContainerManager.class);

  private final AMContext context;
  private final PodLauncher podLauncher;

  private final KubernetesDriverMeta driverMeta;
  private final ApiClient kubernetesApiClient;

  private final Map<Integer, Set<ContainerId>> priorityContainerIdsMap = new ConcurrentHashMap<>();
  private final Queue<ExecutorId> expiredPodQueue = new ConcurrentLinkedQueue<>();

  private boolean isStopped = false;
  private volatile boolean isShuttingDown = false;
  private final KubernetesContainerManagerThread containerManagerThread;

  public KubernetesContainerManager(
      AMContext context,
      KubernetesDriverMeta driverMeta,
      ApiClient kubernetesApiClient
  ) {
    super("kubernetes-container-manager");

    this.context = context;
    this.podLauncher = new PodLauncher(context, driverMeta, kubernetesApiClient);

    this.driverMeta = driverMeta;
    this.kubernetesApiClient = kubernetesApiClient;

    PodChangeWatchThread podChangeWatchThread = new PodChangeWatchThread("podChangeWatcherThread");
    podChangeWatchThread.setDaemon(true);
    podChangeWatchThread.start();

    containerManagerThread = new KubernetesContainerManagerThread();
    containerManagerThread.setName("kubernetesContainerManagerThread");
    containerManagerThread.setDaemon(true);
    containerManagerThread.start();

    LOG.info("KubernetesContainerManager Started");
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
    containerManagerThread.interrupt();
    try {
      containerManagerThread.join();
    } catch (InterruptedException e) {
      // ignore
    }
    super.serviceStop();
  }

  @Override
  public void handle(ContainerManagerEvent event) {
    LOG.info("receiver ContainerManagerEvent:" + event);
    switch (event.getType()) {
      case EXECUTOR_EXPIRED: {
        SchedulerExecutor schedulerExecutor = context
            .getSchedulerExecutorManager()
            .getSchedulerExecutor(event.getContainer().getId().toString());
        if (schedulerExecutor != null) {
          handleReleasedContainer(event.getContainer(), schedulerExecutor.getExecutorExitCode(),
              schedulerExecutor.getExecutorExitMsg());
        }
        expiredPodQueue.add(schedulerExecutor.getExecutorId());
        PrimusMetrics.emitCounterWithAppIdTag(
            "am.container_manager.executor_expired", new HashMap<>(), 1);
        break;
      }
      case GRACEFUL_SHUTDOWN:
      case FORCIBLY_SHUTDOWN:
        LOG.info("Start killing all running containers");
        isShuttingDown = true;
        priorityContainerIdsMap.values().stream()
            .flatMap(Collection::stream)
            .filter(Objects::nonNull)
            .forEach(containerId -> {
              SchedulerExecutor schedulerExecutor = context
                  .getSchedulerExecutorManager()
                  .getSchedulerExecutor(containerId.toString());
              if (event.getType() == GRACEFUL_SHUTDOWN) {
                LOG.info(
                    "Gracefully killing container: {}",
                    schedulerExecutor.getContainer().getId());
                context.emitExecutorKillEvent(schedulerExecutor.getExecutorId());
              } else if (event.getType() == FORCIBLY_SHUTDOWN) {
                LOG.info(
                    "Forcibly killing container: {}",
                    schedulerExecutor.getContainer().getId());
                context.emitExecutorKillForciblyEvent(schedulerExecutor.getExecutorId());
              }
            });
        break;

      case CONTAINER_REQUEST_CREATED:
      case CONTAINER_REQUEST_UPDATED:
        handleContainerRequestCreated();
        break;
    }
  }

  protected void handleReleasedContainer(Container container, int exitStatus, String diag) {
    int priority = container.getPriority().getPriority();
    Set<ContainerId> containerIds = priorityContainerIdsMap.get(priority);
    containerIds.remove(container.getId());
    context.getSchedulerExecutorManager().handle(
        new SchedulerExecutorManagerContainerCompletedEvent(
            SchedulerExecutorManagerEventType.CONTAINER_RELEASED,
            container, exitStatus, diag));
    PrimusMetrics.emitCounterWithAppIdTag(
        "am.container_manager.release_container",
        new HashMap<>(), 1);
  }

  protected void handleContainerRequestCreated() {
    for (Integer priority : context.getRoleInfoManager().getRolePriorities()) {
      priorityContainerIdsMap.putIfAbsent(priority, new HashSet<>());
    }
  }

  class KubernetesContainerManagerThread extends Thread {

    public static final int ALLOCATE_INTERVAL_MS = 5000;

    @Override
    public void run() {
      LOG.info("KubernetesContainerManagerThread Started");
      while (!isStopped) {
        try {
          if (!isShuttingDown) {
            askForContainers();
            cleanExpiredContainer();

            if (context.getSchedulerExecutorManager().isAllSuccess()) {
              finish();
            } else if (context.getSchedulerExecutorManager().isAllCompleted()) {
              String diag = "All executors completed but not success";
              LOG.error(diag);
              abort(diag);
            }
          }
          Thread.sleep(ALLOCATE_INTERVAL_MS);
        } catch (InterruptedException e) {
        }
      }
    }

    private void cleanExpiredContainer() {
      while (!expiredPodQueue.isEmpty()) {
        ExecutorId executorId = expiredPodQueue.poll();
        String podName = ResourceNameBuilder
            .buildExecutorPodName(context.getApplicationMeta().getApplicationId(),
                executorId.toUniqString());
        podLauncher.deleteOnePod(podName);
      }
    }

    protected void abort(String diag) {
      context.emitFailAttemptEvent(diag, ApplicationExitCode.ABORT.getValue());
    }

    protected void finish() {
      LOG.info("All container complete");
      context.emitApplicationSuccessEvent(
          "All container completed",
          ApplicationExitCode.CONTAINER_COMPLETE.getValue());
    }

    private void askForContainers() {
      for (Map.Entry<Integer, Set<ContainerId>> entry : priorityContainerIdsMap.entrySet()) {
        int priority = entry.getKey();
        Set<ContainerId> runningContainers = entry.getValue();
        sendNewContainerRequest(priority, runningContainers);
      }
    }
  }

  private void sendNewContainerRequest(int priority, Set<ContainerId> runningContainers) {
    RoleInfo roleInfo = context.getRoleInfoManager().getRoleInfo(priority);
    int replicas = roleInfo.getRoleSpec().getReplicas();
    int completedReplicaNum = context.getSchedulerExecutorManager().getCompletedNum(priority);
    int runningReplicaNum = runningContainers.size();
    int newContainerToBeRequested = replicas - completedReplicaNum - runningReplicaNum;
    if (newContainerToBeRequested > 0) {
      LOG.info("Role:{}, need to allocated size:{}, total:{}, Completed:{}, Running:{}.",
          roleInfo.getRoleName(), newContainerToBeRequested, replicas, completedReplicaNum,
          runningContainers.size());
    }
    for (int index = 0; index < newContainerToBeRequested; index++) {
      LOG.info("Start request pod index[{}] with role:{}, total required:{}", index,
          roleInfo.getRoleName(), newContainerToBeRequested);
      PodLauncherResult podLauncherResult = podLauncher.createOnePod(roleInfo);
      if (podLauncherResult.isSuccess()) {
        runningContainers.add(podLauncherResult.getContainer().getId());
      } else {
        LOG.warn("Failed to create pod for role:{}, priority:{}", roleInfo.getRoleName(),
            roleInfo.getPriority());
      }
    }
  }

  class PodChangeWatchThread extends Thread {

    public PodChangeWatchThread(String name) {
      super(name);
    }

    public void onReleaseExecutor(int priority, int code, int uniqId) {
      ContainerId containerId = ContainerId.newContainerId(FAKE_YARN_APPLICATION_ID, uniqId);
      Container container = new ContainerPBImpl();
      container.setId(containerId);
      container.setIsGuaranteed(true);
      container.setPriority(Priority.newInstance(priority));

      Set<ContainerId> containerIds = priorityContainerIdsMap.get(priority);
      containerIds.remove(containerId);

      context.getSchedulerExecutorManager().handle(
          new SchedulerExecutorManagerContainerCompletedEvent(
              SchedulerExecutorManagerEventType.CONTAINER_RELEASED,
              container, code, ""));
    }

    @Override
    public void run() {
      KubernetesPodStarter starter = new KubernetesPodStarter(kubernetesApiClient);

      while (true) {
        try {
          Watch<V1Pod> watch = null;
          try {
            watch = starter.createExecutorPodWatch(
                driverMeta.getKubernetesNamespace(),
                context.getApplicationMeta().getApplicationId()
            );
            for (Watch.Response<V1Pod> item : watch) {
              if (item.object == null || item.object.getMetadata() == null) {
                continue;
              }
              LOG.debug("PodName:{}, Method:{}, Status_Phase:{}, APIVersion:{}.",
                  item.object.getMetadata().getName(),
                  item.type,
                  item.object.getStatus().getPhase(),
                  item.object.getMetadata().getResourceVersion());
              if (!item.object.getMetadata().getName().contains("executor")) {
                continue;
              }
              String podName = item.object.getMetadata().getName();
              int executorUniqIndex;
              try {
                String[] split = podName.split("-");
                int length = split.length;
                executorUniqIndex = Integer.valueOf(split[length - 1]);
              } catch (Exception ex) {
                LOG.error("Error when parse Index:" + podName);
                executorUniqIndex = 0;
              }
              int priority;
              Map<String, String> labels = item.object.getMetadata().getLabels();
              if (labels.containsKey(PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME)) {
                priority = Integer.parseInt(labels.get(PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME));
              } else {
                LOG.error("Invalid Primus executor, missing priority, Name: {}", podName);
                continue;
              }
              if (item.object.getStatus().getPhase().equalsIgnoreCase("Succeeded")) {
                LOG.info("POD Success! {}, priority:{}", podName, priority);
                onReleaseExecutor(priority, 0, executorUniqIndex);
              } else if (item.object.getStatus().getPhase().equalsIgnoreCase("Failed")) {
                LOG.info("POD Failed! {}, priority:{}", podName, priority);
                onReleaseExecutor(priority, -1, executorUniqIndex);
              } else if (item.type.equalsIgnoreCase("DELETED")) {
                LOG.info("POD Deleted! {}, priority:{}", podName, priority);
                onReleaseExecutor(priority, -2, executorUniqIndex);
              }
            }
          } catch (Exception exception) {
            if (exception.getCause() instanceof SocketTimeoutException) {
              LOG.debug("timeout when watch pod change");
            }
          } finally {
            try {
              if (watch != null) {
                watch.close();
              }
              Thread.sleep(5000);
            } catch (InterruptedException | IOException e) {

            }
          }
        } catch (Exception ex) {
          LOG.error("Error when PodChangeWatchThread!", ex);
        }
      }
    }
  }
}
