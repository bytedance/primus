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
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticResourceContainerScheduleStrategy implements ContainerScheduleStrategy {
  private static Logger logger = LoggerFactory.getLogger(ElasticResourceContainerScheduleStrategy.class);
  private RoleInfoManager roleInfoManager;
  private Set<Integer> oomCode;
  private static Map<String, Integer> roleElasticResourceInfo = Maps.newConcurrentMap();;

  public ElasticResourceContainerScheduleStrategy(RoleInfoManager roleInfoManager) {
    this.roleInfoManager = roleInfoManager;
    oomCode = Sets.newHashSet(-104);
  }

  @Override
  public ContainerScheduleAction processNewContainer(ContainerScheduleContext scheduleContext) {
    return ContainerScheduleAction.ACCEPT_CONTAINER;
  }

  @Override
  public void postProcess(
      ContainerScheduleAction currentAction, ContainerScheduleContext scheduleContext) {}

  @Override
  public void updateStatus(
      ContainerStatusEnum statusEnum, ContainerScheduleContext scheduleContext) {
    try {
      if (ContainerStatusEnum.RELEASED == statusEnum
          && oomCode.contains(scheduleContext.getExitCode())) {
        int priority = scheduleContext.getContainer().getPriority().getPriority();
        RoleInfo roleInfo = roleInfoManager.getRoleInfo(priority);
        if (isElasticResourceNotSet(roleInfo.getRoleSpec())) {
          return;
        }
        roleInfo.syncResource(roleInfo.getRoleSpec());
        roleElasticResourceInfo.compute(
            roleInfo.getRoleName(),
            (key, value) -> {
              if (value == null) {
                return 1;
              } else {
                scheduleContext.setErrMsg(
                    "[After " + value + " times extend memory] " + scheduleContext.getErrMsg());
                return value + 1;
              }
            });
      }
    } catch (Exception e) {
      logger.error("UpdateStatus failed", e);
    }
  }

  private boolean isElasticResourceNotSet(RoleSpec roleSpec) {
    long maxMemSize = roleSpec.getElasticResource().getMaxMemSize();
    float extendMemRatio = roleSpec.getElasticResource().getExtendMemRatio();
    long extendMemSize = roleSpec.getElasticResource().getExtendMemSize();
    long minMemSize = roleSpec.getElasticResource().getMinMemSize();
    return maxMemSize == 0 && extendMemSize == 0 && minMemSize == 0 && extendMemRatio < 0.00001f;
  }

  public static String getOutOfMemoryMessage() {
    StringBuilder sb = new StringBuilder();
    if (roleElasticResourceInfo.size() != 0) {
      sb.append("<br>Maybe Causes: [");
      for (String role : roleElasticResourceInfo.keySet()) {
        sb.append("<br>Role(")
            .append(role)
            .append(") OOM ")
            .append(roleElasticResourceInfo.get(role))
            .append(" times.");
      }
      sb.append("]");
    }
    return sb.toString();
  }
}
