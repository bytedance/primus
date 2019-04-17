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
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.proto.UtilsProto;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.common.event.EventHandler;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulePolicyManager implements EventHandler<SchedulePolicyManagerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulePolicyManager.class);

  private AMContext context;
  private Map<String, SchedulePolicy> rolePolicyMap;

  public SchedulePolicyManager(AMContext context) {
    this.context = context;
    rolePolicyMap = new HashMap<>();
  }

  @Override
  public void handle(SchedulePolicyManagerEvent event) {
    switch (event.getType()) {
      case POLICY_CREATED:
        for (Map.Entry<String, RoleSpec> entry : event.getRoleSpecMap().entrySet()) {
          RoleSpec roleSpec = entry.getValue();
          rolePolicyMap.put(entry.getKey(), createSchedulePolicy(roleSpec.getSchedulePolicy()));
        }
        break;
    }
  }

  public boolean canSchedule(ExecutorId executorId) {
    SchedulePolicy policy = rolePolicyMap.get(executorId.getRoleName());
    if (policy == null) {
      LOG.warn(executorId.getRoleName() + " not have schedule policy");
      return true;
    }
    return policy.canSchedule(executorId);
  }

  private SchedulePolicy createSchedulePolicy(UtilsProto.SchedulePolicy policy) {
    switch (policy.getSchedulePolicyCase()) {
      case GANGSCHEDULEPOLICY:
        return new GangSchedulePolicyImpl(
            context.getSchedulerExecutorManager(),
            context.getRoleInfoManager()
        );
      case DYNAMICSCHEDULEPOLICY:
        return new DynamicSchedulePolicyImpl(
            context.getSchedulerExecutorManager(),
            context.getRoleInfoManager()
        );
    }
    return null;
  }
}
