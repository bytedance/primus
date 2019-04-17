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

package com.bytedance.primus.am.psonyarn;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.state.InvalidStateTransitonException;
import com.bytedance.primus.common.state.StateMachine;
import com.bytedance.primus.common.state.StateMachineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PonyManager implements EventHandler<PonyEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(PonyManager.class);
  private static final String PS_NAME_PREFIX = "/parameter_server/";

  private AMContext context;
  private SchedulerExecutorManager schedulerExecutorManager;
  private RoleInfoManager roleInfoManager;
  private String psRoleName;
  private boolean allRegisteredFlag = false;
  private Object lock = new Object();

  private static StateMachineFactory<PonyManager, PonyState,
      PonyEventType, PonyEvent> stateMachineFactory =
      new StateMachineFactory<PonyManager, PonyState,
          PonyEventType, PonyEvent>(PonyState.PS_REGISTERING)
          .addTransition(PonyState.PS_REGISTERING,
              PonyState.PS_REGISTERED,
              PonyEventType.PONY_PS_ALL_REGISTERED)
          .addTransition(PonyState.PS_REGISTERED,
              PonyState.RUNNING,
              PonyEventType.PONY_START_WORKER)
          .addTransition(PonyState.RUNNING,
              PonyState.FINISH,
              PonyEventType.PONY_ALL_TASK_COMPLETE)
          .installTopology();
  private final StateMachine<PonyState, PonyEventType,
      PonyEvent> stateMachine;

  public PonyManager(AMContext context, String psRoleName) {
    this.context = context;
    this.roleInfoManager = context.getRoleInfoManager();
    this.psRoleName = psRoleName;
    this.stateMachine = stateMachineFactory.make(this);
  }

  public void setSchedulerExecutorManager(SchedulerExecutorManager schedulerExecutorManager) {
    this.schedulerExecutorManager = schedulerExecutorManager;
  }

  public boolean isAllPsRegistered() {
    int priority = roleInfoManager.getRoleNamePriorityMap().get(psRoleName);
    int registeredNum = schedulerExecutorManager.getRegisteredNum(priority);
    if (roleInfoManager.getPriorityRoleInfoMap().get(priority).getRoleSpec().getReplicas() >
        schedulerExecutorManager.getCompletedNum(priority) + registeredNum) {
      return false;
    } else {
      if (!allRegisteredFlag) {
        allRegisteredFlag = true;
        context.getDispatcher().getEventHandler().handle(
            new PonyEvent(PonyEventType.PONY_PS_ALL_REGISTERED));
      }
      return true;
    }
  }

  public String getPsRoleName() {
    return psRoleName;
  }

  public PonyState getPonyState() {
    synchronized (lock) {
      return stateMachine.getCurrentState();
    }
  }

  @Override
  public void handle(PonyEvent event) {
    synchronized (lock) {
      PonyState oldState = stateMachine.getCurrentState();
      PonyState newState = null;
      try {
        newState = stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]");
      }
      if (oldState != newState) {
        LOG.info("PonyManager transitioned from " + oldState + " to " + newState);
      }
    }
  }
}
