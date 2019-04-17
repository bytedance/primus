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

package com.bytedance.primus.am.schedule.strategy.impl;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleAction;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleContext;
import com.bytedance.primus.am.schedule.strategy.ContainerScheduleStrategy;
import com.bytedance.primus.am.schedule.strategy.ContainerStatusEnum;
import com.bytedance.primus.common.model.records.NodeId;
import java.util.HashMap;
import java.util.Map;

public class MaxContainerPerNodeContainerScheduleStrategy implements ContainerScheduleStrategy {

  private Map<Integer, MaxContainerPerNodeLimiter> perNodeLimiterHashMap = new HashMap<>();
  private RoleInfoManager roleInfoManager;


  public MaxContainerPerNodeContainerScheduleStrategy(RoleInfoManager roleInfoManager) {
    this.roleInfoManager = roleInfoManager;
  }

  @Override
  public ContainerScheduleAction processNewContainer(ContainerScheduleContext scheduleContext) {
    int priority = scheduleContext.getContainer().getPriority().getPriority();
    MaxContainerPerNodeLimiter nodeLimiter = getLimiter(priority);
    NodeId nodeId = scheduleContext.getContainer().getNodeId();
    if (nodeLimiter.isNodeFull(nodeId)) {
      return ContainerScheduleAction.RELEASE_CONTAINER;
    }
    return ContainerScheduleAction.ACCEPT_CONTAINER;
  }

  private MaxContainerPerNodeLimiter getLimiter(int priority) {
    MaxContainerPerNodeLimiter nodeLimiter = perNodeLimiterHashMap.computeIfAbsent(priority, t -> {
      RoleInfo roleInfo = roleInfoManager.getPriorityRoleInfoMap().get(priority);
      int maxReplicasPerNode = roleInfo.getRoleSpec().getScheduleStrategy()
          .getMaxReplicasPerNode();
      return new MaxContainerPerNodeLimiter(maxReplicasPerNode);
    });
    return nodeLimiter;
  }


  @Override
  public void postProcess(ContainerScheduleAction currentAction,
      ContainerScheduleContext scheduleContext) {
    if (currentAction == ContainerScheduleAction.ACCEPT_CONTAINER) {
      int priority = scheduleContext.getContainer().getPriority().getPriority();
      MaxContainerPerNodeLimiter nodeLimiter = getLimiter(priority);
      nodeLimiter.addContainer(scheduleContext.getContainer());
    }
  }

  @Override
  public void updateStatus(ContainerStatusEnum statusEnum,
      ContainerScheduleContext scheduleContext) {
    if (ContainerStatusEnum.RELEASED == statusEnum) {
      int priority = scheduleContext.getContainer().getPriority().getPriority();
      MaxContainerPerNodeLimiter nodeLimiter = getLimiter(priority);
      nodeLimiter.removeContainer(scheduleContext.getContainer());
    }
  }

}
