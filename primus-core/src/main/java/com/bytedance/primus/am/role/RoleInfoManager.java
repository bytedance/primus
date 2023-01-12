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

package com.bytedance.primus.am.role;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.container.ContainerManagerEvent;
import com.bytedance.primus.am.container.ContainerManagerEventType;
import com.bytedance.primus.am.failover.FailoverPolicyManagerEvent;
import com.bytedance.primus.am.failover.FailoverPolicyManagerEventType;
import com.bytedance.primus.am.schedule.SchedulePolicyManagerEvent;
import com.bytedance.primus.am.schedule.SchedulePolicyManagerEventType;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEventType;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoleInfoManager implements EventHandler<RoleInfoManagerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(RoleInfoManager.class);

  private AMContext context;
  private Map<String, Integer> roleNamePriorityMap;
  private Map<String, RoleInfo> roleNameRoleInfoMap;
  private Map<Integer, RoleInfo> priorityRoleInfoMap;
  private int rolePriority;
  private RoleInfoFactory roleInfoFactory;

  public RoleInfoManager(AMContext context) {
    this.context = context;
    roleNameRoleInfoMap = new ConcurrentHashMap<>();
    priorityRoleInfoMap = new ConcurrentHashMap<>();
    roleNamePriorityMap = new ConcurrentHashMap<>();
    rolePriority = 10;
    roleInfoFactory = new DefaultRoleInfoFactory();
  }

  public RoleInfoManager(AMContext context, RoleInfoFactory roleInfoFactory) {
    this(context);
    this.roleInfoFactory = roleInfoFactory;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handle(RoleInfoManagerEvent event) {
    switch (event.getType()) {
      case ROLE_CREATED:
        update(event.getRoleSpecMap());
        context.getDispatcher().getEventHandler()
            .handle(new FailoverPolicyManagerEvent(
                FailoverPolicyManagerEventType.POLICY_CREATED,
                event.getRoleSpecMap()));
        context.getDispatcher().getEventHandler()
            .handle(new SchedulePolicyManagerEvent(
                SchedulePolicyManagerEventType.POLICY_CREATED,
                event.getRoleSpecMap()));
        context.getDispatcher().getEventHandler()
            .handle(new SchedulerExecutorManagerEvent(
                SchedulerExecutorManagerEventType.EXECUTOR_REQUEST_CREATED));
        context.getDispatcher().getEventHandler()
            .handle(new ContainerManagerEvent(
                ContainerManagerEventType.CONTAINER_REQUEST_CREATED));
        break;
      case ROLE_UPDATED:
        update(event.getRoleSpecMap());
        context.getDispatcher().getEventHandler()
            .handle(new FailoverPolicyManagerEvent(
                FailoverPolicyManagerEventType.POLICY_CREATED,
                event.getRoleSpecMap()));
        context.getDispatcher().getEventHandler()
            .handle(new SchedulePolicyManagerEvent(
                SchedulePolicyManagerEventType.POLICY_CREATED,
                event.getRoleSpecMap()));
        context.getDispatcher().getEventHandler()
            .handle(new SchedulerExecutorManagerEvent(
                SchedulerExecutorManagerEventType.EXECUTOR_REQUEST_UPDATED));
        context.getDispatcher().getEventHandler()
            .handle(new ContainerManagerEvent(
                ContainerManagerEventType.CONTAINER_REQUEST_UPDATED));
        break;
    }
  }

  public Map<String, RoleInfo> getRoleNameRoleInfoMap() {
    return roleNameRoleInfoMap;
  }

  public Map<Integer, RoleInfo> getPriorityRoleInfoMap() {
    return priorityRoleInfoMap;
  }

  public Map<String, Integer> getRoleNamePriorityMap() {
    return roleNamePriorityMap;
  }

  public String getTaskManagerName(ExecutorId executorId) {
    return roleNameRoleInfoMap.get(executorId.getRoleName()).getDataStreamName();
  }

  private void update(Map<String, RoleSpec> roleSpecMap) {
    for (Map.Entry<String, RoleSpec> entry : roleSpecMap.entrySet()) {
      String roleName = entry.getKey();
      int priority = getRolePriority(roleName);
      RoleSpec roleSpec = entry.getValue();
      RoleInfo roleInfo = roleInfoFactory.createRoleInfo(roleSpecMap.keySet(), roleName, roleSpec,
          priority);
      PrimusMetrics.emitStoreWithAppIdTag("role.replica",
          new HashMap<String, String>() {{
            put("role", roleName);
          }},
          roleInfo.getRoleSpec().getReplicas());
      roleNamePriorityMap.put(roleName, priority);
      priorityRoleInfoMap.put(priority, roleInfo);
      roleNameRoleInfoMap.put(roleName, roleInfo);
    }
  }

  private int getRolePriority(String roleName) {
    if (roleNamePriorityMap.containsKey(roleName)) {
      return roleNamePriorityMap.get(roleName);
    } else {
      return rolePriority++;
    }
  }

  private class DefaultRoleInfoFactory implements RoleInfoFactory {

    public RoleInfo createRoleInfo(
        Iterable<String> roleNames,
        String roleName,
        RoleSpec roleSpec,
        int priority
    ) {
      return new RoleInfo(roleNames, roleName, roleSpec, priority);
    }
  }
}
