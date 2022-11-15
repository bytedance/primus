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

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_EXECUTOR_PRIORITY_LABEL_NAME;

import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.bytedance.primus.am.container.ContainerManagerEvent;
import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerContainerCompletedEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEventType;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.ApplicationId;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;
import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.runtime.kubernetesnative.am.KubernetesAMContext;
import com.bytedance.primus.runtime.kubernetesnative.am.PodLauncher;
import com.bytedance.primus.runtime.kubernetesnative.am.PodLauncherResult;
import com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants;
import com.bytedance.primus.runtime.kubernetesnative.common.pods.KubernetesPodStarter;
import com.bytedance.primus.runtime.kubernetesnative.common.utils.ResourceNameBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesContainerManager extends AbstractService implements
    EventHandler<ContainerManagerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesContainerManager.class);

  public static final ApplicationAttemptId FAKE_YARN_APPLICATION_ID = ApplicationAttemptId
      .newInstance(
          ApplicationId.fromString(KubernetesConstants.FAKE_YARN_APPLICATION_NAME),
          KubernetesConstants.FAKE_APPLICATION_ATTEMPT_ID);

  private KubernetesAMContext context;
  protected Map<Integer, Set<ContainerId>> priorityContainerIdsMap;
  protected RoleInfoManager roleInfoManager;
  protected SchedulerExecutorManager schedulerExecutorManager;

  private boolean isStopped = false;
  private KubernetesContainerManagerThread containerManagerThread;
  protected volatile boolean gracefulShutdown = false;
  private PodLauncher podLauncher;

  protected Queue<ExecutorId> expiredPodQueue;

  public KubernetesContainerManager(KubernetesAMContext context, String name) {
    super(name);
    roleInfoManager = context.getRoleInfoManager();
    this.context = context;
    priorityContainerIdsMap = new ConcurrentHashMap<>();
    schedulerExecutorManager = context.getSchedulerExecutorManager();
    podLauncher = new PodLauncher(context);
    expiredPodQueue = new ConcurrentLinkedQueue<>();

    PodChangeWatchThread podChangeWatchThread = new PodChangeWatchThread("podChangeWatcherThread");
    podChangeWatchThread.setDaemon(true);
    podChangeWatchThread.start();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("KubernetesContainerManager Started");
    containerManagerThread = new KubernetesContainerManagerThread();
    containerManagerThread.setName("kubernetesContainerManagerThread");
    containerManagerThread.setDaemon(true);
    containerManagerThread.start();
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
        SchedulerExecutor schedulerExecutor =
            schedulerExecutorManager.getSchedulerExecutor(event.getContainer().getId().toString());
        if (schedulerExecutor != null) {
          handleReleasedContainer(event.getContainer(), schedulerExecutor.getExecutorExitCode(),
              schedulerExecutor.getExecutorExitMsg());
        }
        expiredPodQueue.add(schedulerExecutor.getExecutorId());
        PrimusMetrics.emitCounterWithOptionalPrefix("am.container_manager.executor_expired", 1);
        break;
      }
      case GRACEFUL_SHUTDOWN: {
        LOG.info("Graceful shutdown! Kill all running containers");
        gracefulShutdown = true;
        for (Entry<Integer, Set<ContainerId>> runningContainers : priorityContainerIdsMap
            .entrySet()) {
          for (ContainerId containerId : runningContainers.getValue()) {
            LOG.info("Killing container " + containerId);
            SchedulerExecutor schedulerExecutor =
                schedulerExecutorManager.getSchedulerExecutor(containerId.toString());
            context.getDispatcher().getEventHandler()
                .handle(new SchedulerExecutorManagerEvent(
                    SchedulerExecutorManagerEventType.EXECUTOR_KILL,
                    schedulerExecutor.getExecutorId()));
          }
        }
        break;
      }
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
    schedulerExecutorManager.handle(
        new SchedulerExecutorManagerContainerCompletedEvent(
            SchedulerExecutorManagerEventType.CONTAINER_RELEASED,
            container, exitStatus, diag));
    PrimusMetrics.emitCounterWithOptionalPrefix("am.container_manager.release_container", 1);
  }

  protected void handleContainerRequestCreated() {
    for (Integer priority : roleInfoManager.getPriorityRoleInfoMap().keySet()) {
      priorityContainerIdsMap.putIfAbsent(priority, new HashSet<>());
    }
  }

  public static ContainerId getContainerIdByUniqId(long uniqId) {
    return ContainerId.newContainerId(KubernetesContainerManager.FAKE_YARN_APPLICATION_ID, uniqId);
  }

  class KubernetesContainerManagerThread extends Thread {

    public static final int ALLOCATE_INTERVAL_MS = 5000;

    @Override
    public void run() {
      LOG.info("KubernetesContainerManagerThread Started");
      while (!isStopped) {
        try {
          if (!gracefulShutdown) {
            askForContainers();
            cleanExpiredContainer();

            if (schedulerExecutorManager.isAllSuccess()) {
              finish();
            } else if (schedulerExecutorManager.isAllCompleted()) {
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
            .buildExecutorPodName(context.getAppId(), executorId.toUniqString());
        podLauncher.deleteOnePod(podName);
      }
    }

    protected void abort(String diag) {
      context.getDispatcher().getEventHandler().handle(
          new ApplicationMasterEvent(context, ApplicationMasterEventType.FAIL_ATTEMPT, diag,
              ApplicationExitCode.ABORT.getValue()));
    }

    protected void finish() {
      LOG.info("All container complete");
      context.getDispatcher().getEventHandler().handle(
          new ApplicationMasterEvent(context,
              ApplicationMasterEventType.SUCCESS,
              "All container completed",
              ApplicationExitCode.CONTAINER_COMPLETE.getValue()));
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
    RoleInfo roleInfo = roleInfoManager.getPriorityRoleInfoMap().get(priority);
    int replicas = roleInfo.getRoleSpec().getReplicas();
    int completedReplicaNum = schedulerExecutorManager.getCompletedNum(priority);
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

      schedulerExecutorManager.handle(
          new SchedulerExecutorManagerContainerCompletedEvent(
              SchedulerExecutorManagerEventType.CONTAINER_RELEASED,
              container, code, ""));
    }

    @Override
    public void run() {
      KubernetesPodStarter starter = new KubernetesPodStarter(context);

      while (true) {
        try {
          Watch<V1Pod> watch = null;
          try {
            watch = starter.createExecutorPodWatch(
                context.getKubernetesNamespace(),
                context.getAppId()
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
