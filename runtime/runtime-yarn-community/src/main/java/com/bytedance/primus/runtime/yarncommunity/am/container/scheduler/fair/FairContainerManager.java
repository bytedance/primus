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

package com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.fair;

import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.runtime.yarncommunity.am.YarnAMContext;
import com.bytedance.primus.runtime.yarncommunity.am.container.YarnContainerManager;
import com.bytedance.primus.runtime.yarncommunity.am.container.launcher.ContainerLauncherEvent;
import com.bytedance.primus.runtime.yarncommunity.am.container.launcher.ContainerLauncherEventType;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FairContainerManager extends YarnContainerManager {

  public static final Logger LOG = LoggerFactory.getLogger(FairContainerManager.class);

  protected Map<Integer, Integer> priorityNeedRequestNumMap;
  private int maxAllocationNumEachRound;

  public FairContainerManager(YarnAMContext context) {
    super(context);
    maxAllocationNumEachRound =
        context.getPrimusConf().getScheduler().getMaxAllocationNumEachRound();
    maxAllocationNumEachRound =
        (maxAllocationNumEachRound <= 0) ? Integer.MAX_VALUE : maxAllocationNumEachRound;
    priorityNeedRequestNumMap = new HashMap<>();
  }

  @Override
  protected void updatePriorityContainerIdsMap() {
    super.updatePriorityContainerIdsMap();
    for (Integer priority : roleInfoManager.getPriorityRoleInfoMap().keySet()) {
      priorityNeedRequestNumMap.putIfAbsent(priority, 0);
    }
  }

  @Override
  protected void handleAllocation(AllocateResponse response) {
    int allocatedNum = 0;
    for (Container container : response.getAllocatedContainers()) {
      int priority = container.getPriority().getPriority();
      logContainerUrl(container);
      Integer needNum = priorityNeedRequestNumMap.get(priority);
      if (needNum == null) {
        LOG.warn("priorityNeedRequestNumMap do not has priority[" + priority + "]");
        amRMClient.releaseAssignedContainer(container.getId());
        continue;
      }
      --needNum;
      if (needNum < 0) {
        needNum = 0;
      }
      priorityNeedRequestNumMap.put(priority, needNum);

      if (allocatedNum >= maxAllocationNumEachRound) {
        LOG.warn("Already allocate " + allocatedNum
            + " container this round, exceed max allocation num "
            + maxAllocationNumEachRound);
        amRMClient.releaseAssignedContainer(container.getId());
        continue;
      }

      Set<ContainerId> containerIds = priorityContainerIdsMap.get(priority);
      if (containerIds == null) {
        LOG.warn("priorityContainerIdsMap do not has priority[" + priority + "]");
        amRMClient.releaseAssignedContainer(container.getId());
        continue;
      }

      FairRoleInfo roleInfo = (FairRoleInfo) roleInfoManager.getPriorityRoleInfoMap().get(priority);
      if (roleInfo == null) {
        LOG.warn("priorityRoleInfoMap do not has priority[" + priority + "]");
        amRMClient.releaseAssignedContainer(container.getId());
        continue;
      }

      int completedNum = schedulerExecutorManager.getCompletedNum(priority);
      int replicas = roleInfo.getRoleSpec().getReplicas();
      if (containerIds.size() + completedNum >= replicas || isShuttingDown) {
        LOG.info("Enough role: " + roleInfo.getRoleName() + ", num: " + containerIds.size()
            + ", graceful shutdown: " + isShuttingDown);
        amRMClient.releaseAssignedContainer(container.getId());
        amRMClient.removeContainerRequest(roleInfo.getContainerRequest());
        PrimusMetrics.emitCounterWithAppIdTag(
            "am.container_manager.remove_request", new HashMap<>(), 1);
        continue;
      }

      containerIds.add(container.getId());
      runningContainerMap.put(container.getId(), container);
      LOG.info("Send launch event for container[" + container + "]");
      context.getDispatcher().getEventHandler().handle(
          new ContainerLauncherEvent(container, ContainerLauncherEventType.CONTAINER_ALLOCATED));
      allocatedNum++;
    }
  }

  @Override
  protected void askForContainers() {
    for (Map.Entry<Integer, ConcurrentSkipListSet<ContainerId>> entry : priorityContainerIdsMap.entrySet()) {
      int priority = entry.getKey();
      FairRoleInfo roleInfo = (FairRoleInfo) roleInfoManager.getPriorityRoleInfoMap().get(priority);
      int replicas = roleInfo.getRoleSpec().getReplicas();
      int completedReplicaNum = schedulerExecutorManager.getCompletedNum(priority);
      int runningReplicaNum = entry.getValue().size();

      int needNum = priorityNeedRequestNumMap.get(priority);
      int newContainerToBeRequested = replicas - completedReplicaNum - runningReplicaNum - needNum;
      if (newContainerToBeRequested > 0) {
        LOG.info("AskContainers for role:[" + roleInfo.getRoleName() + "], total: " + replicas
            + ", running: " + runningReplicaNum + ", completed: " + completedReplicaNum + ", need: "
            + newContainerToBeRequested);
      }
      for (int index = 0; index < newContainerToBeRequested; index++) {
        ContainerRequest containerRequest = roleInfo.getContainerRequest();
        amRMClient.addContainerRequest(containerRequest);
        PrimusMetrics.emitCounterWithAppIdTag(
            "am.container_manager.add_request", new HashMap<>(), 1);
      }
      priorityNeedRequestNumMap.put(priority, needNum + newContainerToBeRequested);
    }
  }


  protected void setPriorityContainerIdsMap(Map<Integer, ConcurrentSkipListSet<ContainerId>> map) {
    priorityContainerIdsMap = map;
  }

  protected void setAmClient(AMRMClient<AMRMClient.ContainerRequest> client) {
    amRMClient = client;
  }

  protected Map<Integer, Integer> getPriorityNeedRequestNumMap() {
    return priorityNeedRequestNumMap;
  }
}
