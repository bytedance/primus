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

import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.api.records.ExecutorCommand;
import com.bytedance.primus.api.records.impl.pb.ExecutorCommandPBImpl;
import com.bytedance.primus.apiserver.proto.UtilsProto.InputPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ScheduleStrategy.RoleCategory;
import com.bytedance.primus.apiserver.proto.UtilsProto.YarnScheduler;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.records.impl.RoleSpecImpl;
import java.util.Map;

public class RoleInfo {

  private static final String ROLES_LIST_ENV_KEY = "ROLES_LIST";
  private static final String ROLE_ENV_KEY_PREFIX = "NUM_OF_PRIMUS_";
  private static final String TOTAL_ORACLES_ENV_KEY = "TOTAL_ORACLES";
  private static final String PRIMUS_ROLE_CATEGORY_KEY = "PRIMUS_ROLE_CATEGORY";

  private String roleName;
  private RoleSpec roleSpec;
  private int priority;
  private ExecutorCommand command;
  private YarnScheduler roleScheduler;

  public RoleInfo(Iterable<String> roleNames, String roleName, RoleSpec roleSpec, int priority) {
    this.roleScheduler = roleSpec.getRoleScheduler();
    this.roleName = roleName;
    // Make a copy and do not change the origin when update environments in the following
    this.roleSpec = new RoleSpecImpl(roleSpec.getProto());
    this.priority = priority;
    command = new ExecutorCommandPBImpl();
    command.setCommand(this.roleSpec.getExecutorSpecTemplate().getCommand());

    // Update environments
    Map<String, String> envs = this.roleSpec.getExecutorSpecTemplate().getEnvs();
    envs.put(PRIMUS_ROLE_CATEGORY_KEY, getRoleCategory().toLowerCase());
    envs.put(ROLES_LIST_ENV_KEY, String.join(",", roleNames));
    envs.put(ROLE_ENV_KEY_PREFIX + roleName, String.valueOf(this.roleSpec.getReplicas()));
    envs.put(TOTAL_ORACLES_ENV_KEY, String.valueOf(this.roleSpec.getReplicas()));
    command.setEnvironment(envs);
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public RoleSpec getRoleSpec() {
    return roleSpec;
  }

  public void setRoleSpec(RoleSpec roleSpec) {
    this.roleSpec = roleSpec;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public ExecutorCommand getCommand() {
    return command;
  }

  public void setCommand(ExecutorCommand command) {
    this.command = command;
  }

  public YarnScheduler getRoleScheduler() {
    return roleScheduler;
  }

  public void setRoleScheduler(YarnScheduler roleScheduler) {
    this.roleScheduler = roleScheduler;
  }

  public String getDataStreamName() {
    InputPolicy inputPolicy = roleSpec.getExecutorSpecTemplate().getInputPolicy();
    switch (inputPolicy.getInputPolicyCase()) {
      case STREAMINGINPUTPOLICY:
        return inputPolicy.getStreamingInputPolicy().getDataStream();
      case ENVINPUTPOLICY:
      default:
        return DataStreamManager.ENV_DATA_STREAM;
    }
  }

  public void syncResource(RoleSpec roleSpec) {
  }

  public String getRoleCategory() {
    RoleCategory roleCategory = roleSpec.getScheduleStrategy().getRoleCategory();
    String roleCategoryStr = roleCategory.name();
    if (roleCategory == RoleCategory.UNKNOWN || roleCategory == RoleCategory.UNRECOGNIZED) {
      if (roleName.contains("ps")) {
        roleCategoryStr = RoleCategory.PS.name();
      } else {
        roleCategoryStr = RoleCategory.WORKER.name();
      }
    }
    return roleCategoryStr;
  }
}
