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

package com.bytedance.primus.am.schedulerexecutor;

import com.bytedance.blacklist.BlacklistTracker;
import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.eventlog.ExecutorCompleteEvent;
import com.bytedance.primus.am.eventlog.ExecutorEvent;
import com.bytedance.primus.am.eventlog.ExecutorStartEvent;
import com.bytedance.primus.am.failover.FailoverPolicyManager;
import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedule.SchedulePolicyManager;
import com.bytedance.primus.api.protocolrecords.RegisterRequest;
import com.bytedance.primus.api.protocolrecords.RegisterResponse;
import com.bytedance.primus.api.protocolrecords.impl.pb.RegisterResponsePBImpl;
import com.bytedance.primus.api.records.ClusterSpec;
import com.bytedance.primus.api.records.ExecutorCommandType;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.ExecutorSpecs;
import com.bytedance.primus.api.records.ExecutorState;
import com.bytedance.primus.api.records.impl.pb.ClusterSpecPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorSpecPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorSpecsPBImpl;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.NodeId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.impl.pb.ContainerPBImpl;
import com.bytedance.primus.common.network.NetworkConfig;
import com.bytedance.primus.common.service.AbstractService;
import com.bytedance.primus.common.util.AbstractLivelinessMonitor;
import com.bytedance.primus.executor.ExecutorExitCode;
import com.bytedance.primus.proto.PrimusConfOuterClass.NetworkConfig.NetworkType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Function;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerExecutorManager extends AbstractService
    implements EventHandler<SchedulerExecutorManagerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerExecutorManager.class);

  private AMContext context;
  private RoleInfoManager roleInfoManager;
  private SchedulePolicyManager schedulePolicyManager;
  private FailoverPolicyManager failoverPolicyManager;
  private AbstractLivelinessMonitor executorMonitor;

  private Map<String, ExecutorSpec[]> clusterSpec;

  private Map<Integer, Integer> priorityRequestNumMap;
  private Map<Integer, Integer> priorityFinishNumMap;

  private Map<Integer, BitSet> priorityExecutorIndexesMap;
  private Map<Integer, Integer> priorityCompletedNumMap;
  private Map<Integer, Integer> prioritySuccessNumMap;
  private Map<String, ExecutorId> containerExecutorMap;
  private Map<ExecutorId, SchedulerExecutor> runningExecutorMap;
  private Map<ExecutorId, String> registeredExecutorUniqIdMap;
  private List<SchedulerExecutor> completedExecutors;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private AtomicLong uniqId = new AtomicLong(0);

  private NetworkConfig networkConfig;

  public SchedulerExecutorManager(AMContext context) {
    super(SchedulerExecutorManager.class.getName());
    this.context = context;
    roleInfoManager = context.getRoleInfoManager();
    failoverPolicyManager = context.getFailoverPolicyManager();
    schedulePolicyManager = context.getSchedulePolicyManager();
    executorMonitor = context.getMonitor();
    clusterSpec = new ConcurrentHashMap<>();
    priorityRequestNumMap = new HashMap<>();
    priorityFinishNumMap = new HashMap<>();
    priorityExecutorIndexesMap = new HashMap<>();
    priorityCompletedNumMap = new ConcurrentHashMap<>();
    prioritySuccessNumMap = new ConcurrentHashMap<>();
    containerExecutorMap = new ConcurrentHashMap<>();
    runningExecutorMap = new ConcurrentHashMap<>();
    registeredExecutorUniqIdMap = new ConcurrentHashMap<>();

    networkConfig = new NetworkConfig(context.getPrimusConf());

    completedExecutors = new ArrayList<>();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  public Map<Integer, Integer> getPrioritySuccessNumMap() {
    return prioritySuccessNumMap;
  }

  public Map<Integer, Integer> getPriorityFinishNumMap() {
    return priorityFinishNumMap;
  }

  public Map<String, ExecutorId> getContainerExecutorMap() {
    return containerExecutorMap;
  }

  public List<SchedulerExecutor> getCompletedExecutors() {
    return completedExecutors;
  }

  public Map<ExecutorId, SchedulerExecutor> getRunningExecutorMap() {
    return runningExecutorMap;
  }

  public SchedulerExecutor getSchedulerExecutor(String containerId) {
    ExecutorId executorId = containerExecutorMap.get(containerId);
    return getSchedulerExecutor(executorId);
  }

  public SchedulerExecutor getSchedulerExecutor(ExecutorId executorId) {
    if (executorId == null) {
      return null;
    }
    return runningExecutorMap.get(executorId);
  }

  public int getCompletedNum(int priority) {
    return priorityCompletedNumMap.getOrDefault(priority, 0);
  }

  public int getSucceedNum(int priority) {
    return prioritySuccessNumMap.getOrDefault(priority, 0);
  }

  public int getFailedNum(int priority) {
    return getCompletedNum(priority) - getSucceedNum(priority);
  }

  private boolean isEmptyRequest() {
    return priorityRequestNumMap.isEmpty() ||
        priorityRequestNumMap.values().stream().mapToInt(Integer::intValue).sum() <= 0;
  }

  public boolean isAllSuccess() {
    try {
      readLock.lock();
      if (isEmptyRequest()) {
        return false;
      }
      for (int priority : priorityRequestNumMap.keySet()) {
        if (prioritySuccessNumMap.get(priority) < priorityFinishNumMap.get(priority)) {
          return false;
        }
      }
      return true;
    } finally {
      readLock.unlock();
    }
  }

  public boolean isAllCompleted() {
    try {
      readLock.lock();
      if (isEmptyRequest()) {
        return false;
      }
      for (Integer priority : priorityRequestNumMap.keySet()) {
        if (getCompletedNum(priority) < priorityRequestNumMap.get(priority)) {
          return false;
        }
      }
      return true;
    } finally {
      readLock.unlock();
    }
  }

  public int getRegisteredNum(int priority) {
    try {
      readLock.lock();
      int registeredNum = 0;
      for (SchedulerExecutor executor : runningExecutorMap.values()) {
        if (executor.getPriority() == priority && executor.getSpec() != null) {
          ++registeredNum;
        }
      }
      return registeredNum;
    } finally {
      readLock.unlock();
    }
  }

  private static ContainerId getContainerIdByUniqIdForK8s(
      ApplicationAttemptId applicationAttemptId,
      long uniqId
  ) {
    return ContainerId.newContainerId(applicationAttemptId, uniqId);
  }

  /**
   * Only for Kubernetes to create mock ExecutorId
   *
   * @param roleInfo
   * @param applicationAttemptId
   * @param podNameGenerator
   * @return
   */
  public ExecutorId createExecutorForK8s( // TODO: Move to kubernetes runtime
      RoleInfo roleInfo,
      ApplicationAttemptId applicationAttemptId,
      Function<String, String> podNameGenerator
  ) {
    writeLock.lock();
    try {
      Integer priority = roleInfo.getPriority();
      BitSet bitSet = priorityExecutorIndexesMap.get(priority);
      int executorIndex = bitSet.nextClearBit(0);
      if (executorIndex == -1) {
        return null;
      }
      bitSet.set(executorIndex);
      ExecutorId executorId = new ExecutorIdPBImpl();
      String roleName = roleInfoManager.getPriorityRoleInfoMap().get(priority).getRoleName();
      executorId.setRoleName(roleName);
      executorId.setIndex(executorIndex);
      long uniqId = this.uniqId.incrementAndGet();
      executorId.setUniqId(uniqId);
      Container container = new ContainerPBImpl();
      ContainerId containerId = getContainerIdByUniqIdForK8s(applicationAttemptId, uniqId);
      container.setId(containerId);
      container.setPriority(Priority.newInstance(roleInfo.getPriority()));
      container.setNodeHttpAddress("N/A");
      container.setNodeId(NodeId.newInstance(podNameGenerator.apply(executorId.toUniqString()), 0));

      runningExecutorMap.put(executorId, new SchedulerExecutorImpl(context, executorId, container));
      containerExecutorMap.put(container.getId().toString(), executorId);
      LOG.info("runningExecutorMap put [{}], containerExecutorMap put [{}]", executorId,
          container.getId());
      ExecutorEvent executorEvent = new ExecutorStartEvent(Integer.toString(executorIndex),
          executorId);
      context.getDispatcher().getEventHandler().handle(executorEvent);
      PrimusMetrics.emitStoreWithAppIdTag(
          "am.executor_num",
          new HashMap<String, String>() {{
            put("role", roleName);
          }},
          bitSet.cardinality());
      return executorId;
    } catch (Exception e) {
      LOG.warn("Failed to create executor", e);
      return null;
    } finally {
      writeLock.unlock();
    }

  }

  public ExecutorId createExecutor(Container container) {
    writeLock.lock();
    try {
      Integer priority = container.getPriority().getPriority();
      BitSet bitSet = priorityExecutorIndexesMap.get(priority);
      int executorIndex = bitSet.nextClearBit(0);
      if (executorIndex == -1) {
        return null;
      }

      if (context.getBlacklistTrackerOpt().isPresent() && isContainerHostInBlacklist(container,
          context.getBlacklistTrackerOpt().get())) {
        LOG.info("Current container's Host is in Blacklist, Id:{}, Host:{}",
            container.getId().toString(), container.getNodeId().getHost());
        return null;
      }

      bitSet.set(executorIndex);
      ExecutorId executorId = new ExecutorIdPBImpl();
      String roleName = roleInfoManager.getPriorityRoleInfoMap().get(priority).getRoleName();
      executorId.setRoleName(roleName);
      executorId.setIndex(executorIndex);
      executorId.setUniqId(uniqId.incrementAndGet());
      runningExecutorMap.put(executorId, new SchedulerExecutorImpl(context, executorId, container));
      LOG.info("runningExecutorMap put [" + executorId + "]");
      containerExecutorMap.put(container.getId().toString(), executorId);
      LOG.info("containerExecutorMap put [" + container.getId() + "]");
      context.getExecutorNodeMap()
          .put(executorId.toUniqString(), container.getNodeId().getHost());
      ExecutorEvent executorEvent =
          new ExecutorStartEvent(container.getId().toString(), executorId);
      context.getDispatcher().getEventHandler().handle(executorEvent);
      PrimusMetrics.emitStoreWithAppIdTag("am.executor_num",
          new HashMap<String, String>() {{
            put("role", roleName);
          }},
          bitSet.cardinality());
      return executorId;
    } catch (Exception e) {
      LOG.warn("Failed to create executor", e);
      return null;
    } finally {
      writeLock.unlock();
    }
  }

  private boolean isContainerHostInBlacklist(Container container,
      BlacklistTracker blacklistTrackerOpt) {
    try {
      writeLock.lock();
      Set<String> latestNodeBlackList = blacklistTrackerOpt.getNodeBlacklist().keySet();
      String hostName = container.getNodeId().getHost();
      return latestNodeBlackList.contains(hostName);
    } catch (Exception e) {
      LOG.error("Error when check isContainerHostInBlacklist()", e);
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  public RegisterResponse register(RegisterRequest request) {
    this.readLock.lock();
    try {
      RegisterResponse response = new RegisterResponsePBImpl();
      ExecutorId executorId = request.getExecutorId();
      registeredExecutorUniqIdMap.put(executorId, executorId.toUniqString());
      if (!runningExecutorMap.containsKey(executorId)) {
        LOG.warn("Scheduler does not has executor [" + executorId + "]");
        return response;
      }
      SchedulerExecutor schedulerExecutor = runningExecutorMap.get(executorId);

      if (schedulerExecutor.getSpec() == null) {
        if (enableIpAndPortFailover() && executorNotFirstRun(executorId)) {
          ExecutorSpec previousExecutorSpec = findPreviousExecutorSpec(executorId).get();
          updateRegisterRequestWithPreviousSpec(request, previousExecutorSpec);
        }
        schedulerExecutor.setSpec(request.getExecutorSpec());
        updateClusterSpec(request.getExecutorSpec());
        schedulerExecutor.handle(new SchedulerExecutorEvent(SchedulerExecutorEventType.REGISTERED));
        executorMonitor.register(executorId);
      } else {
        executorMonitor.receivedPing(executorId);
      }

      if (schedulerExecutor.getExecutorState() != SchedulerExecutorState.KILLING) {
        if (!schedulePolicyManager.canSchedule(executorId)) {
          LOG.warn("Cannot schedule executor[" + executorId +
              "], not all needed executor registered");
          return response;
        }
      }

      response.setClusterSpec(getClusterSpec());
      response.setCommand(
          roleInfoManager.getRoleNameRoleInfoMap().get(executorId.getRoleName()).getCommand());
      return response;
    } finally {
      readLock.unlock();
    }
  }

  protected void updateRegisterRequestWithPreviousSpec(RegisterRequest request,
      ExecutorSpec previousExecutorSpec) {
    ExecutorSpec requestExecutorSpec = request.getExecutorSpec();
    LOG.info("OverlayNetwork enabled, request executor spec: {}", requestExecutorSpec);
    requestExecutorSpec.setEndpoints(previousExecutorSpec.getEndpoints());
    request.setExecutorSpec(requestExecutorSpec);
    LOG.info("OverlayNetwork enabled, replaced executor spec: {}", request.getExecutorSpec());
  }

  private boolean enableIpAndPortFailover() {
    return NetworkType.OVERLAY == networkConfig.getNetworkType()
        && networkConfig.isKeepIpAndPortUnderOverlay();
  }

  private Optional<ExecutorSpec> findPreviousExecutorSpec(ExecutorId executorId) {
    if (!clusterSpec.containsKey(executorId.getRoleName())) {
      return Optional.empty();
    }
    ExecutorSpec[] executorSpecs = clusterSpec.get(executorId.getRoleName());
    for (ExecutorSpec spec : executorSpecs) {
      if (spec != null && spec.getExecutorId() != null && spec.getExecutorId().equals(executorId)) {
        return Optional.of(spec);
      }
    }
    return Optional.empty();
  }

  private boolean executorNotFirstRun(ExecutorId executorId) {
    return findPreviousExecutorSpec(executorId).isPresent();
  }

  public ExecutorCommandType heartbeat(ExecutorId executorId, ExecutorState executorState) {
    executorMonitor.receivedPing(executorId);
    readLock.lock();
    try {
      ExecutorCommandType executorCommandType = ExecutorCommandType.NONE;
      SchedulerExecutor schedulerExecutor = runningExecutorMap.get(executorId);
      if (schedulerExecutor == null) {
        LOG.warn("can not find executor [" + executorId + "] in heartbeat");
        return executorCommandType;
      }

      String node = context.getExecutorNodeMap().get(executorId.toUniqString());
      boolean isBlacklisted = context.getBlacklistTrackerOpt().map(
              b -> b.isContainerBlacklisted(executorId.toUniqString()) ||
                  b.isNodeBlacklisted(node))
          .orElse(false);
      boolean isExecutorKilling =
          schedulerExecutor.getExecutorState() == SchedulerExecutorState.KILLING;
      String uniqueId = registeredExecutorUniqIdMap.get(executorId);
      if (uniqueId != null && (!uniqueId.equals(executorId.toUniqString()))) {
        isExecutorKilling = true;
      }
      if (isBlacklisted) {
        LOG.info("ExecutorId[" + executorId + "] is in blacklist. Send blacklisted event");
        schedulerExecutor
            .handle(new SchedulerExecutorEvent(SchedulerExecutorEventType.BLACKLIST));
        return executorCommandType;
      } else if (isExecutorKilling) {
        LOG.info("ExecutorId[" + executorId + "] is in killing.");
      }
      if (isExecutorKilling) {
        executorCommandType = ExecutorCommandType.KILL;
      } else if (executorState == ExecutorState.REGISTERED) {
        executorCommandType = ExecutorCommandType.START;
      } else if (executorState == ExecutorState.RUNNING) {
        schedulerExecutor.handle(new SchedulerExecutorEvent(SchedulerExecutorEventType.STARTED));
      }
      return executorCommandType;
    } finally {
      readLock.unlock();
    }
  }

  public void unregister(ExecutorId executorId, int exitCode, String exitMsg) {
    readLock.lock();
    try {
      String uniqueId = registeredExecutorUniqIdMap.get(executorId);
      if (uniqueId != null && (!uniqueId.equals(executorId.toUniqString()))) {
        return;
      }
      registeredExecutorUniqIdMap.remove(executorId);
      executorMonitor.unregister(executorId);
      SchedulerExecutor schedulerExecutor = runningExecutorMap.get(executorId);
      if (schedulerExecutor == null) {
        LOG.warn("Can not find executor [" + executorId + "] in unregister, " +
            "exit code[" + exitCode + "], " + "exit msg[" + exitMsg + "]");
        return;
      }
      SchedulerExecutorEventType eventType;
      if (exitCode == 0) {
        eventType = SchedulerExecutorEventType.SUCCEEDED;
      } else if (exitCode == ExecutorExitCode.KILLED.getValue()) {
        eventType = SchedulerExecutorEventType.KILLED;
      } else {
        eventType = SchedulerExecutorEventType.FAILED;
      }
      LOG.info("Executor [" + executorId + "] unregister, event type " + eventType);
      schedulerExecutor.handle(new SchedulerExecutorCompletedEvent(exitCode, exitMsg, eventType));
    } finally {
      readLock.unlock();
    }
  }

  private void updateClusterSpec(ExecutorSpec executorSpec) {
    // Put executor spec into cluster spec according to role name and role index
    String roleName = executorSpec.getExecutorId().getRoleName();
    int roleIndex = executorSpec.getExecutorId().getIndex();
    int replicas = roleInfoManager.getRoleNameRoleInfoMap().get(roleName).getRoleSpec()
        .getReplicas();
    synchronized (clusterSpec) {
      ExecutorSpec[] executorSpecArray = clusterSpec.get(roleName);
      if (executorSpecArray == null) {
        executorSpecArray = new ExecutorSpecPBImpl[replicas];
        clusterSpec.put(roleName, executorSpecArray);
      } else if (replicas > executorSpecArray.length) {
        // TODO: Should we compact array when replicas < array's length?
        executorSpecArray = Arrays.copyOf(executorSpecArray, replicas);
        clusterSpec.put(roleName, executorSpecArray);
      }
      clusterSpec.get(roleName)[roleIndex] = executorSpec;
    }
  }

  public ClusterSpec getClusterSpec() {
    Map<String, ExecutorSpecs> executorSpecsMap = new HashMap<>();
    synchronized (clusterSpec) {
      for (Map.Entry<String, ExecutorSpec[]> entry : clusterSpec.entrySet()) {
        ExecutorSpecs executorSpecs = new ExecutorSpecsPBImpl();
        executorSpecs.setExecutorSpecs(Arrays.asList(entry.getValue()));
        executorSpecsMap.put(entry.getKey(), executorSpecs);
      }
    }
    ClusterSpec tmpClusterSpec = new ClusterSpecPBImpl();
    tmpClusterSpec.setExecutorSpecs(executorSpecsMap);
    return tmpClusterSpec;
  }

  @Override
  public void handle(SchedulerExecutorManagerEvent e) {
    try {
      writeLock.lock();
      switch (e.getType()) {
        case CONTAINER_KILLED:
        case CONTAINER_RELEASED: {
          SchedulerExecutorManagerContainerCompletedEvent event =
              (SchedulerExecutorManagerContainerCompletedEvent) e;
          String containerId = event.getContainer().getId().toString();

          if (!containerExecutorMap.containsKey(containerId)) {
            LOG.warn("can not find container id [" + containerId + "] in scheduler");
          } else {
            LOG.info("Container({}) is completed with {}", containerId, e.getType().toString());

            ExecutorId executorId = containerExecutorMap.remove(containerId);
            executorMonitor.unregister(executorId);
            TaskManager taskManager = context.getDataStreamManager()
                .getTaskManager(roleInfoManager.getTaskManagerName(executorId));
            if (taskManager != null) {
              taskManager.unregister(executorId);
            } else {
              LOG.warn("Cannot get task manager for " + executorId.toString()
                  + ", task manager is not ready");
            }
            SchedulerExecutor schedulerExecutor = runningExecutorMap.remove(executorId);
            completedExecutors.add(schedulerExecutor);
            SchedulerExecutorState oldScheduleExecutorState =
                schedulerExecutor.getExecutorState();
            if (event.getType() == SchedulerExecutorManagerEventType.CONTAINER_RELEASED) {
              schedulerExecutor.handle(
                  new SchedulerExecutorReleasedEvent(event.getExitCode(), event.getExitMsg()));
            }
            Integer priority = event.getContainer().getPriority().getPriority();
            if (oldScheduleExecutorState == SchedulerExecutorState.NEW) {
              LOG.info("Container({}) is completed as new container.", containerId);
              priorityExecutorIndexesMap.get(priority)
                  .clear(schedulerExecutor.getExecutorId().getIndex());
            } else {
              LOG.info("Container({}) requires failover handling", containerId);

              failoverPolicyManager.increaseFailoverTimes(schedulerExecutor);
              if (failoverPolicyManager.needFailover(schedulerExecutor)) {
                LOG.info("Container({}) requires failover handling", containerId);

                priorityExecutorIndexesMap.get(priority)
                    .clear(schedulerExecutor.getExecutorId().getIndex());
              } else {

                int completedNum = priorityCompletedNumMap.get(priority);
                priorityCompletedNumMap.put(priority, completedNum + 1);
                if (schedulerExecutor.isSuccess()) {
                  LOG.info("Container({}) completed successfully", containerId);
                  int successNum = prioritySuccessNumMap.get(priority);
                  prioritySuccessNumMap.put(priority, successNum + 1);
                }
              }
            }
            if (!schedulerExecutor.isSuccess()) {
              LOG.info("Container({}) didn't complete successfully.", containerId);
              String node = context.getExecutorNodeMap().get(executorId.toUniqString());
              context.getBlacklistTrackerOpt().ifPresent(b -> b.addContainerFailure(
                  executorId.toUniqString(),
                  node,
                  System.currentTimeMillis()
              ));
            }
            context.getExecutorNodeMap().remove(executorId.toUniqString());
            ExecutorEvent executorEvent = new ExecutorCompleteEvent(context, schedulerExecutor);
            context.getDispatcher().getEventHandler().handle(executorEvent);
          }
          PrimusMetrics.emitCounterWithAppIdTag(
              "am.scheduler_manager.container_released",
              new HashMap<>(),
              1);
          break;
        }
        case EXECUTOR_EXPIRED: {
          ExecutorId executorId = e.getExecutorId();
          executorMonitor.unregister(executorId);
          SchedulerExecutor schedulerExecutor = runningExecutorMap.get(executorId);
          if (schedulerExecutor == null) {
            LOG.warn("can not find expired executor " + executorId);
          } else {
            schedulerExecutor.handle(
                new SchedulerExecutorEvent(SchedulerExecutorEventType.EXPIRED));
          }
          PrimusMetrics.emitCounterWithAppIdTag(
              "am.scheduler_manager.executor_expired",
              new HashMap<>(),
              1);
          break;
        }
        case EXECUTOR_REQUEST_CREATED: {
          for (RoleInfo roleInfo : roleInfoManager.getPriorityRoleInfoMap().values()) {
            int priority = roleInfo.getPriority();
            int roleNum = roleInfo.getRoleSpec().getReplicas();
            priorityRequestNumMap.putIfAbsent(priority, roleNum);
            float successPercent = roleInfo.getRoleSpec().getSuccessPolicy().getSuccessPercent();
            priorityFinishNumMap.put(priority, (int) (roleNum * successPercent + 99) / 100);
            priorityExecutorIndexesMap.putIfAbsent(priority, new BitSet(roleNum));
            priorityCompletedNumMap.putIfAbsent(priority, 0);
            prioritySuccessNumMap.putIfAbsent(priority, 0);
          }
          break;
        }
        case EXECUTOR_REQUEST_UPDATED: {
          for (RoleInfo roleInfo : roleInfoManager.getPriorityRoleInfoMap().values()) {
            int priority = roleInfo.getPriority();
            int roleNum = roleInfo.getRoleSpec().getReplicas();
            int oldRequestNum = priorityRequestNumMap.getOrDefault(priority, 0);
            if (roleNum < oldRequestNum) {
              killExecutors(priority, roleNum, oldRequestNum, false /* force */);
            }
            priorityRequestNumMap.put(priority, roleNum);
            float successPercent = roleInfo.getRoleSpec().getSuccessPolicy().getSuccessPercent();
            priorityFinishNumMap.put(priority, (int) (roleNum * successPercent + 99) / 100);
            priorityExecutorIndexesMap.putIfAbsent(priority, new BitSet(roleNum));
            priorityCompletedNumMap.putIfAbsent(priority, 0);
            prioritySuccessNumMap.putIfAbsent(priority, 0);
          }
          break;
        }
        case EXECUTOR_KILL:
          killExecutor(e.getExecutorId(), false /* force */);
          break;
        case EXECUTOR_KILL_FORCIBLY:
          killExecutor(e.getExecutorId(), true /* force */);
          break;
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void killExecutors(int priority, int startIndex, int endIndex, boolean force) {
    for (; startIndex < endIndex; startIndex++) {
      ExecutorId executorId = new ExecutorIdPBImpl();
      String roleName = roleInfoManager.getPriorityRoleInfoMap().get(priority).getRoleName();
      executorId.setRoleName(roleName);
      executorId.setIndex(startIndex);
      killExecutor(executorId, force);
    }
  }

  private void killExecutor(ExecutorId executorId, boolean force) {
    SchedulerExecutor schedulerExecutor = runningExecutorMap.get(executorId);
    if (schedulerExecutor != null) {
      LOG.info("Killing executor " + executorId);
      schedulerExecutor.handle(force
          ? new SchedulerExecutorEvent(SchedulerExecutorEventType.KILL_FORCIBLY)
          : new SchedulerExecutorEvent(SchedulerExecutorEventType.KILL));
    }
  }

  public boolean isZombie(ExecutorId executorId) {
    String expectedRegisteredExecutorUniqId =
        registeredExecutorUniqIdMap.getOrDefault(executorId, executorId.toUniqString());
    return !expectedRegisteredExecutorUniqId.equals(executorId.toUniqString());
  }
}
