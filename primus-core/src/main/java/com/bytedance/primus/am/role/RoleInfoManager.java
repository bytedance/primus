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
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Introduce locks to this class
// TODO: Maybe not using priority for role UUID
public class RoleInfoManager implements EventHandler<RoleInfoManagerEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(RoleInfoManager.class);

  private final AMContext context;
  private final RoleInfoFactory roleInfoFactory;

  // XXX: Having three ConcurrentHashMaps doesn't ensure the consistency among them.
  private final Map<String, Integer> roleNamePriorityMap = new ConcurrentHashMap<>();
  private final Map<String, RoleInfo> roleNameRoleInfoMap = new ConcurrentHashMap<>();
  private final Map<Integer, RoleInfo> priorityRoleInfoMap = new ConcurrentHashMap<>();

  private int rolePriority = 10; // Monotonically increasing for new roles

  public RoleInfoManager(AMContext context) {
    this.context = context;
    this.roleInfoFactory = new DefaultRoleInfoFactory();
  }

  public RoleInfoManager(AMContext context, RoleInfoFactory roleInfoFactory) {
    this.context = context;
    this.roleInfoFactory = roleInfoFactory;
  }

  @Override
  public void handle(RoleInfoManagerEvent event) {
    switch (event.getType()) {
      case ROLE_CREATED:
        update(event.getRoleSpecMap());
        context.emitRoleCreatedSubsequentEvents(event.getRoleSpecMap());
        break;

      case ROLE_UPDATED:
        update(event.getRoleSpecMap());
        context.emitRoleUpdatedSubsequentEvents(event.getRoleSpecMap());
        break;

      default:
        throw new IllegalArgumentException("Unsupported EventType: " + event.getType());
    }
  }

  public RoleInfo getRoleInfo(int priority) {
    return priorityRoleInfoMap.get(priority);
  }

  public RoleInfo getRoleInfo(ExecutorId executorId) {
    return getRoleInfo(executorId.getRoleName());
  }

  public RoleInfo getRoleInfo(String roleName) {
    return roleNameRoleInfoMap.get(roleName);
  }

  public Collection<Integer> getRolePriorities() {
    return priorityRoleInfoMap.keySet();
  }

  public Collection<String> getRoleNames() {
    return roleNameRoleInfoMap.keySet();
  }

  public Collection<RoleInfo> getRoleInfos() {
    return priorityRoleInfoMap.values();
  }

  public String getTaskManagerName(ExecutorId executorId) {
    return getRoleInfo((executorId)).getDataStreamName();
  }

  public int getRolePriority(ExecutorId executorId) {
    return roleNamePriorityMap.get(executorId.getRoleName());
  }

  private void update(Map<String, RoleSpec> roleSpecMap) {
    for (Map.Entry<String, RoleSpec> entry : roleSpecMap.entrySet()) {
      String roleName = entry.getKey();
      int priority = getRolePriority(roleName);
      RoleSpec roleSpec = entry.getValue();
      RoleInfo roleInfo = roleInfoFactory
          .createRoleInfo(
              roleSpecMap.keySet(),
              roleName,
              roleSpec,
              priority
          );

      roleNamePriorityMap.put(roleName, priority);
      priorityRoleInfoMap.put(priority, roleInfo);
      roleNameRoleInfoMap.put(roleName, roleInfo);

      PrimusMetrics.emitStoreWithAppIdTag(
          "role.replica",
          new HashMap<String, String>() {{
            put("role", roleName);
          }},
          roleInfo.getRoleSpec().getReplicas());
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
