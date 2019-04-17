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
import com.bytedance.primus.apiserver.records.Condition;
import com.bytedance.primus.apiserver.records.JobStatus;
import com.bytedance.primus.apiserver.records.RoleStatus;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobStatusImpl implements JobStatus {

  private ResourceProto.JobStatus proto = ResourceProto.JobStatus.getDefaultInstance();
  private ResourceProto.JobStatus.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, RoleStatus> roleStatuses;
  private List<Condition> conditions;

  public JobStatusImpl() {
    builder = ResourceProto.JobStatus.newBuilder();
  }

  public JobStatusImpl(ResourceProto.JobStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized JobStatus setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
    return this;
  }

  @Override
  public synchronized long getStartTime() {
    ResourceProto.JobStatusOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public synchronized JobStatus setCompletionTime(long completionTime) {
    maybeInitBuilder();
    builder.setCompletionTime(completionTime);
    return this;
  }

  @Override
  public synchronized long getCompletionTime() {
    ResourceProto.JobStatusOrBuilder p = viaProto ? proto : builder;
    return p.getCompletionTime();
  }

  @Override
  public synchronized JobStatus setRoleStatuses(Map<String, RoleStatus> roleStatuses) {
    maybeInitBuilder();
    if (roleStatuses == null) {
      builder.clearRoleStatuses();
    }
    this.roleStatuses = roleStatuses;
    return this;
  }

  @Override
  public synchronized Map<String, RoleStatus> getRoleStatuses() {
    if (roleStatuses != null) {
      return roleStatuses;
    }
    ResourceProto.JobStatusOrBuilder p = viaProto ? proto : builder;
    roleStatuses = p.getRoleStatusesMap().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> new RoleStatusImpl(e.getValue()))
    );
    return roleStatuses;
  }

  @Override
  public synchronized JobStatus setConditions(List<Condition> conditions) {
    maybeInitBuilder();
    if (conditions == null) {
      builder.clearConditions();
    }
    this.conditions = conditions;
    return this;
  }

  @Override
  public synchronized List<Condition> getConditions() {
    if (conditions != null) {
      return conditions;
    }
    ResourceProto.JobStatusOrBuilder p = viaProto ? proto : builder;
    conditions =
        p.getConditionsList().stream().map(e -> new ConditionImpl(e)).collect(Collectors.toList());
    return conditions;
  }

  @Override
  public synchronized ResourceProto.JobStatus getProto() {
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
    if (!(obj instanceof JobStatusImpl)) {
      return false;
    }

    JobStatusImpl other = (JobStatusImpl) obj;
    boolean result = true;
    result = result && (getStartTime() == other.getStartTime());
    result = result && (getCompletionTime() == other.getCompletionTime());
    result = result && (getRoleStatuses().equals(other.getRoleStatuses()));
    result = result && (getConditions().equals(other.getConditions()));
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
      builder = ResourceProto.JobStatus.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (roleStatuses != null) {
      addRoleStatusesToProto();
    }
    if (conditions != null) {
      addConditionsToProto();
    }
  }

  private synchronized void addRoleStatusesToProto() {
    maybeInitBuilder();
    builder.clearRoleStatuses();
    if (roleStatuses == null) {
      return;
    }
    builder.putAllRoleStatuses(
        roleStatuses.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getProto())
        )
    );
  }

  private synchronized void addConditionsToProto() {
    maybeInitBuilder();
    builder.clearConditions();
    if (conditions == null) {
      return;
    }
    builder.addAllConditions(conditions.stream()
        .map(e -> e.getProto()).collect(Collectors.toList()));
  }
}
