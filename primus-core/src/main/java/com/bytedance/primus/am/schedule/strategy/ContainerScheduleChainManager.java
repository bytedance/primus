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

package com.bytedance.primus.am.schedule.strategy;

import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedule.strategy.impl.AddHostToBlacklistByContainerExitCodeStrategy;
import com.bytedance.primus.am.schedule.strategy.impl.ElasticResourceContainerScheduleStrategy;
import com.bytedance.primus.am.schedule.strategy.impl.MaxContainerPerNodeContainerScheduleStrategy;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerScheduleChainManager implements ContainerScheduleActionCombiner {

  private static final Logger log = LoggerFactory.getLogger(ContainerScheduleChainManager.class);

  private List<ContainerScheduleStrategy> processChain = new ArrayList<>();

  public ContainerScheduleChainManager(RoleInfoManager roleInfoManager) {
    processChain.add(new MaxContainerPerNodeContainerScheduleStrategy(roleInfoManager));
    processChain.add(new ElasticResourceContainerScheduleStrategy(roleInfoManager));
    processChain.add(new AddHostToBlacklistByContainerExitCodeStrategy());
  }

  public ContainerScheduleAction processNewContainer(ContainerScheduleContext context) {
    try {
      List<ContainerScheduleAction> actionList = new ArrayList<>();
      for (int index = 0; index < processChain.size(); index++) {
        ContainerScheduleStrategy scheduleStrategy = processChain.get(index);
        ContainerScheduleAction processAction = scheduleStrategy.processNewContainer(context);
        actionList.add(processAction);
      }
      ContainerScheduleAction action = combineAction(actionList);
      postProcessAction(action, context);
      return action;
    } catch (Exception ex) {
      log.error("error when processNewContainer, context: " + context, ex);
    }
    return ContainerScheduleAction.ACCEPT_CONTAINER;
  }

  public void processReleasedContainer(ContainerScheduleContext context) {
    try {
      for (int i = 0; i < processChain.size(); i++) {
        ContainerScheduleStrategy scheduleStrategy = processChain.get(i);
        scheduleStrategy.updateStatus(ContainerStatusEnum.RELEASED, context);
      }
    } catch (Exception ex) {
      log.error("error when processReleasedContainer, context: " + context, ex);
    }
  }

  public void postProcessAction(ContainerScheduleAction currentAction,
      ContainerScheduleContext scheduleContext) {
    try {
      for (int i = 0; i < processChain.size(); i++) {
        ContainerScheduleStrategy scheduleStrategy = processChain.get(i);
        scheduleStrategy.postProcess(currentAction, scheduleContext);
      }
    } catch (Exception ex) {
      log.error("error when postProcessAction, context: " + scheduleContext, ex);
    }

  }

  @Override
  public ContainerScheduleAction combineAction(List<ContainerScheduleAction> actionList) {
    if (actionList.contains(ContainerScheduleAction.RELEASE_CONTAINER)) {
      return ContainerScheduleAction.RELEASE_CONTAINER;
    }
    if (actionList.stream().allMatch(t -> t == ContainerScheduleAction.ACCEPT_CONTAINER)) {
      return ContainerScheduleAction.ACCEPT_CONTAINER;
    }
    return ContainerScheduleAction.ACCEPT_CONTAINER;
  }
}
