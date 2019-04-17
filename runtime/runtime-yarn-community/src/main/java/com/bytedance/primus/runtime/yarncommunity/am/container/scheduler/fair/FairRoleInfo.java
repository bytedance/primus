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

package com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.fair;

import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.runtime.yarncommunity.am.container.YarnRoleInfo;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FairRoleInfo extends YarnRoleInfo {

  private static final Logger LOG = LoggerFactory.getLogger(FairRoleInfo.class);

  protected ContainerRequest containerRequest;

  public FairRoleInfo(
      Iterable<String> roleNames,
      String roleName,
      RoleSpec roleSpec,
      int priority
  ) {
    super(roleNames, roleName, roleSpec, priority);
    containerRequest = new ContainerRequest(
        resource, null /* nodes */, null /* racks */,
        Priority.newInstance(priority));
  }

  public ContainerRequest getContainerRequest() {
    return containerRequest;
  }

  @Override
  public void syncResource(RoleSpec roleSpec) {
    super.syncResource(roleSpec);
    containerRequest = new ContainerRequest(
        resource, null /* nodes */, null /* racks */,
        Priority.newInstance(getPriority()));
  }
}