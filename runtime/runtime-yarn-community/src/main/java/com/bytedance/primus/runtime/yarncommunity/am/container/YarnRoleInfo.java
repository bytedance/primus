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

package com.bytedance.primus.runtime.yarncommunity.am.container;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.records.RoleSpec;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class YarnRoleInfo extends RoleInfo {

  private static final Logger LOG = LoggerFactory.getLogger(YarnRoleInfo.class);

  protected Resource resource;

  public YarnRoleInfo(Iterable<String> roleNames, String roleName, RoleSpec roleSpec,
      int priority) {
    super(roleNames, roleName, roleSpec, priority);
    resource = createResource(roleSpec);
  }

  public Resource getResource() {
    return resource;
  }

  protected Resource createResource(RoleSpec roleSpec) {
    Resource resource = Records.newRecord(Resource.class);
    for (ResourceRequest request : roleSpec.getExecutorSpecTemplate().getResourceRequests()) {
      switch (request.getResourceType()) {
        case VCORES:
          resource.setVirtualCores(request.getValue());
          break;
        case MEMORY_MB:
          resource.setMemorySize(request.getValue());
          break;
        default:
          LOG.warn("Unsupported resource type " + request.getResourceType());
      }
    }
    return resource;
  }

  @Override
  public void syncResource(RoleSpec roleSpec) {
    LOG.info(
        "Before sync mem:{}, extendMemRatio:{}, extendMemSize:{}, maxMemSize:{}, minMemSize:{}",
        resource.getMemorySize(),
        roleSpec.getElasticResource().getExtendMemRatio(),
        roleSpec.getElasticResource().getExtendMemSize(),
        roleSpec.getElasticResource().getMaxMemSize(),
        roleSpec.getElasticResource().getMinMemSize());
    long maxSize = Long.MAX_VALUE;
    long minSize = 1;
    if (roleSpec.getElasticResource().getMaxMemSize() > 0) {
      maxSize = roleSpec.getElasticResource().getMaxMemSize();
    }
    if (roleSpec.getElasticResource().getMinMemSize() > 0) {
      minSize = roleSpec.getElasticResource().getMinMemSize();
    }
    long newSize;
    if (roleSpec.getElasticResource().getExtendMemRatio() > 0.00001f) {
      LOG.info("Use ratio {} to sync mem", roleSpec.getElasticResource().getExtendMemRatio());
      newSize = Math.round(
          resource.getMemorySize() * roleSpec.getElasticResource().getExtendMemRatio());
    } else {
      LOG.info("Ratio not set,use size {} to sync mem",
          roleSpec.getElasticResource().getExtendMemSize());
      newSize = Math.round(
          resource.getMemorySize() + roleSpec.getElasticResource().getExtendMemSize());
    }
    resource.setMemorySize(Math.max(minSize, Math.min(maxSize, newSize)));
    LOG.info("Extended memory of {} to {} ", getRoleName(), resource.getMemorySize());
  }
}