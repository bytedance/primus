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

package com.bytedance.primus.am.schedule;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.proto.UtilsProto.SchedulePolicy.SchedulePolicyCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GangSchedulePolicyImpl implements SchedulePolicy {

  private static final Logger LOG = LoggerFactory.getLogger(GangSchedulePolicyImpl.class);

  private SchedulerExecutorManager schedulerExecutorManager;
  private RoleInfoManager roleManager;
  private boolean isAllRoleGang;

  public GangSchedulePolicyImpl(
      SchedulerExecutorManager schedulerExecutorManager,
      RoleInfoManager roleInfoManager) {
    this.schedulerExecutorManager = schedulerExecutorManager;
    this.roleManager = roleInfoManager;
    this.isAllRoleGang = true;
    for (RoleInfo roleInfo : roleInfoManager.getRoleInfos()) {
      if (roleInfo.getRoleSpec().getSchedulePolicy().getSchedulePolicyCase()
          != SchedulePolicyCase.GANGSCHEDULEPOLICY) {
        isAllRoleGang = false;
        break;
      }
    }
  }

  @Override
  public boolean canSchedule(ExecutorId executorId) {
    if (isAllRoleGang) {
      for (String roleName : roleManager.getRoleNames()) {
        if (!canScheduleRole(roleName)) {
          return false;
        }
      }
      return true;
    } else {
      return canScheduleRole(executorId.getRoleName());
    }
  }

  private boolean canScheduleRole(String roleName) {
    RoleInfo roleInfo = roleManager.getRoleInfo(roleName);
    int registeredNum = schedulerExecutorManager.getRegisteredNum(roleInfo.getPriority());
    int completedNum = schedulerExecutorManager.getCompletedNum(roleInfo.getPriority());
    if (roleInfo.getRoleSpec().getReplicas() > completedNum + registeredNum) {
      return false;
    }
    return true;
  }
}