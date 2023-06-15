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

package com.bytedance.primus.apiserver.records.impl;

import com.bytedance.primus.apiserver.proto.ResourceProto;
import com.bytedance.primus.apiserver.records.JobSpec;
import com.bytedance.primus.apiserver.records.RoleSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class JobSpecImpl implements JobSpec {

  private ResourceProto.JobSpec proto = ResourceProto.JobSpec.getDefaultInstance();
  private ResourceProto.JobSpec.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, RoleSpec> roleSpecs;

  public JobSpecImpl() {
    builder = ResourceProto.JobSpec.newBuilder();
  }

  public JobSpecImpl(ResourceProto.JobSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized JobSpec setRoleSpecs(Map<String, RoleSpec> roleSpecs) {
    maybeInitBuilder();
    if (roleSpecs == null) {
      builder.clearRoleSpecs();
    }
    this.roleSpecs = roleSpecs;
    return this;
  }

  @Override
  public synchronized Map<String, RoleSpec> getRoleSpecs() {
    if (roleSpecs != null) {
      return roleSpecs;
    }
    ResourceProto.JobSpecOrBuilder p = viaProto ? proto : builder;
    roleSpecs = new HashMap<>(p.getRoleSpecsMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new RoleSpecImpl(e.getValue()))));
    return roleSpecs;
  }

  @Override
  public synchronized ResourceProto.JobSpec getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof JobSpecImpl)) {
      return false;
    }

    JobSpecImpl other = (JobSpecImpl) obj;
    boolean result = true;
    result = result && (getRoleSpecs().equals(other.getRoleSpecs()));
    return result;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.JobSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (roleSpecs != null) {
      addRoleSpecsToProto();
    }
  }

  private synchronized void addRoleSpecsToProto() {
    maybeInitBuilder();
    builder.clearRoleSpecs();
    if (roleSpecs == null) {
      return;
    }
    builder.putAllRoleSpecs(
        roleSpecs.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getProto()))
    );
  }
}
