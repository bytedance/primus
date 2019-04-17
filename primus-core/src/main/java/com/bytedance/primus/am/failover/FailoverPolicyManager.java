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

package com.bytedance.primus.am.failover;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.apiserver.proto.UtilsProto;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.common.event.EventHandler;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverPolicyManager implements EventHandler<FailoverPolicyManagerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(FailoverPolicyManager.class);

  private AMContext context;
  private Map<String, FailoverPolicy> rolePolicyMap;

  public FailoverPolicyManager(AMContext context) {
    this.context = context;
    rolePolicyMap = new HashMap<>();
  }

  @Override
  public void handle(FailoverPolicyManagerEvent event) {
    switch (event.getType()) {
      case POLICY_CREATED:
        for (Map.Entry<String, RoleSpec> entry : event.getRoleSpecMap().entrySet()) {
          RoleSpec roleSpec = entry.getValue();
          rolePolicyMap.put(entry.getKey(), createFailoverPolicy(roleSpec.getFailoverPolicy()));
        }
        break;
    }
  }

  public boolean needFailover(SchedulerExecutor schedulerExecutor) {
    FailoverPolicy policy = rolePolicyMap.get(schedulerExecutor.getExecutorId().getRoleName());
    if (policy == null) {
      LOG.warn(schedulerExecutor.getExecutorId().getRoleName() + " not have failover policy");
      return false;
    }
    if (schedulerExecutor.isSuccess()) {
      return policy.needFailover(schedulerExecutor, false);
    } else {
      return policy.needFailover(schedulerExecutor, true);
    }
  }

  public void increaseFailoverTimes(SchedulerExecutor schedulerExecutor) {
    FailoverPolicy policy = rolePolicyMap.get(schedulerExecutor.getExecutorId().getRoleName());
    if (policy != null) {
      policy.increaseFailoverTimes(schedulerExecutor);
    }
  }

  private FailoverPolicy createFailoverPolicy(UtilsProto.FailoverPolicy policy) {
    switch (policy.getFailoverPolicyCase()) {
      case COMMONFAILOVERPOLICY:
        return new CommonFailoverPolicyImpl(context, policy.getCommonFailoverPolicy());
      case EXITCODEFAILOVERPOLICY:
        return new ExitCodeFailoverPolicyImpl(context, policy.getExitCodeFailoverPolicy());
    }
    return null;
  }
}