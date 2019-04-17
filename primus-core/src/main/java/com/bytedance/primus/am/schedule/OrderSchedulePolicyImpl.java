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

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.role.RoleInfoManager;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManager;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.proto.PrimusConfOuterClass.OrderSchedulePolicy.RolePolicy;
import java.util.List;

public class OrderSchedulePolicyImpl implements SchedulePolicy {

  private AMContext context;
  private SchedulerExecutorManager schedulerExecutorManager;
  private RoleInfoManager roleInfoManager;
  private List<RolePolicy> rolePolicyList;

  public OrderSchedulePolicyImpl(AMContext context,
      SchedulerExecutorManager schedulerExecutorManager) {
    this.context = context;
    this.schedulerExecutorManager = schedulerExecutorManager;
    this.roleInfoManager = context.getRoleInfoManager();
    this.rolePolicyList = context.getPrimusConf().getScheduler().getSchedulePolicy()
        .getOrderPolicy().getRolePolicyList();
  }

  @Override
  public boolean canSchedule(ExecutorId executorId) {

    /*
    for (RolePolicy rolePolicy : rolePolicyList) {
      String roleName = rolePolicy.getRoleName();
      int priority = roleManager.getRoleNamePriorityMap().get(roleName);
      int registeredNum = schedulerExecutorManager.getRegisteredNum(priority);
      if (!roleName.equals(executorId.getRoleName())) {
        if (roleManager.getPriorityRoleInfoMap().get(priority).getRoleConf().getNum() >
            schedulerExecutorManager.getCompletedNum(priority) + registeredNum) {
          return false;
        }
      } else {
        switch (rolePolicy.getScheduleType()) {
          case DYNAMIC:
            return true;
          case GANG:
            if (roleManager.getPriorityRoleInfoMap().get(priority).getRoleConf().getNum() >
                schedulerExecutorManager.getCompletedNum(priority) + registeredNum) {
              return false;
            } else {
              return true;
            }
        }
      }
    }

    return false;

     */
    return true;
  }
}
