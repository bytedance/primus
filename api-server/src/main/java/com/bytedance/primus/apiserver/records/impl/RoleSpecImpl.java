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
import com.bytedance.primus.apiserver.proto.UtilsProto.ElasticResource;
import com.bytedance.primus.apiserver.proto.UtilsProto.FailoverPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.SchedulePolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ScheduleStrategy;
import com.bytedance.primus.apiserver.proto.UtilsProto.SuccessPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.YarnScheduler;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import com.bytedance.primus.apiserver.records.RoleSpec;

public class RoleSpecImpl implements RoleSpec {

  private ResourceProto.RoleSpec proto = ResourceProto.RoleSpec.getDefaultInstance();
  private ResourceProto.RoleSpec.Builder builder = null;
  private boolean viaProto = false;

  private ExecutorSpec executorSpecTemplate;
  private SchedulePolicy schedulePolicy;
  private FailoverPolicy failoverPolicy;
  private SuccessPolicy successPolicy;
  private YarnScheduler roleScheduler;
  private ScheduleStrategy scheduleStrategy;

  public RoleSpecImpl() {
    builder = ResourceProto.RoleSpec.newBuilder();
  }

  public RoleSpecImpl(ResourceProto.RoleSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized RoleSpec setReplicas(int replicas) {
    maybeInitBuilder();
    builder.setReplicas(replicas);
    return this;
  }

  @Override
  public synchronized int getReplicas() {
    ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
    return p.getReplicas();
  }

  @Override
  public synchronized RoleSpec setMinReplicas(int minReplicas) {
    maybeInitBuilder();
    builder.setMinReplicas(minReplicas);
    return this;
  }

  @Override
  public synchronized int getMinReplicas() {
    ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
    return p.getMinReplicas();
  }

  @Override
  public synchronized RoleSpec setExecutorSpecTemplate(ExecutorSpec executorSpecTemplate) {
    maybeInitBuilder();
    if (executorSpecTemplate == null) {
      builder.clearExecutorSpecTemplate();
    }
    this.executorSpecTemplate = executorSpecTemplate;
    return this;
  }

  @Override
  public synchronized ExecutorSpec getExecutorSpecTemplate() {
    if (executorSpecTemplate != null) {
      return executorSpecTemplate;
    }
    ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
    executorSpecTemplate = new ExecutorSpecImpl(p.getExecutorSpecTemplate());
    return executorSpecTemplate;
  }

  @Override
  public synchronized RoleSpec setSchedulePolicy(SchedulePolicy schedulePolicy) {
    maybeInitBuilder();
    if (schedulePolicy == null) {
      builder.clearSchedulePolicy();
    }
    this.schedulePolicy = schedulePolicy;
    return this;
  }

  @Override
  public synchronized SchedulePolicy getSchedulePolicy() {
    if (schedulePolicy != null) {
      return schedulePolicy;
    }
    ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
    schedulePolicy = p.getSchedulePolicy();
    return schedulePolicy;
  }

  @Override
  public synchronized RoleSpec setFailoverPolicy(FailoverPolicy failoverPolicy) {
    maybeInitBuilder();
    if (failoverPolicy == null) {
      builder.clearFailoverPolicy();
    }
    this.failoverPolicy = failoverPolicy;
    return this;
  }

  @Override
  public synchronized FailoverPolicy getFailoverPolicy() {
    if (failoverPolicy != null) {
      return failoverPolicy;
    }
    ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
    failoverPolicy = p.getFailoverPolicy();
    return failoverPolicy;
  }

  @Override
  public synchronized RoleSpec setSuccessPolicy(SuccessPolicy successPolicy) {
    maybeInitBuilder();
    if (successPolicy == null) {
      builder.clearSuccessPolicy();
    }
    this.successPolicy = successPolicy;
    return this;
  }

  @Override
  public synchronized SuccessPolicy getSuccessPolicy() {
    if (successPolicy != null) {
      return successPolicy;
    }
    ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
    successPolicy = p.getSuccessPolicy();
    return successPolicy;
  }

  @Override
  public synchronized ScheduleStrategy getScheduleStrategy() {
    if (scheduleStrategy != null) {
      return scheduleStrategy;
    }
    ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
    scheduleStrategy = p.getScheduleStrategy();
    return scheduleStrategy;
  }

  @Override
  public synchronized RoleSpec setScheduleStrategy(ScheduleStrategy scheduleStrategy) {
    maybeInitBuilder();
    if (scheduleStrategy == null) {
      builder.clearScheduleStrategy();
    }
    this.scheduleStrategy = scheduleStrategy;
    return this;
  }

  @Override
  public synchronized ResourceProto.RoleSpec getProto() {
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
    if (!(obj instanceof RoleSpecImpl)) {
      return false;
    }

    RoleSpecImpl other = (RoleSpecImpl) obj;
    boolean result = true;
    result = result && (getReplicas() == other.getReplicas());
    result = result && (getMinReplicas() == other.getMinReplicas());
    result = result && (getExecutorSpecTemplate().equals(other.getExecutorSpecTemplate()));
    result = result && (getSchedulePolicy().equals(other.getSchedulePolicy()));
    result = result && (getFailoverPolicy().equals(other.getFailoverPolicy()));
    result = result && (getSuccessPolicy().equals(other.getSuccessPolicy()));
    result = result && (getScheduleStrategy().equals(other.getScheduleStrategy()));
    result = result && (getRoleScheduler().equals(other.getRoleScheduler()));
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
      builder = ResourceProto.RoleSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (executorSpecTemplate != null) {
      builder.setExecutorSpecTemplate(executorSpecTemplate.getProto());
    }
    if (schedulePolicy != null) {
      builder.setSchedulePolicy(schedulePolicy);
    }
    if (failoverPolicy != null) {
      builder.setFailoverPolicy(failoverPolicy);
    }
    if (successPolicy != null) {
      builder.setSuccessPolicy(successPolicy);
    }
    if (scheduleStrategy != null) {
      builder.setScheduleStrategy(scheduleStrategy);
    }
    if (roleScheduler != null) {
      builder.setRoleScheduler(roleScheduler);
    }
  }

  @Override
  public RoleSpec setRoleScheduler(YarnScheduler scheduler) {
    if (scheduler == null) {
      builder.clearRoleScheduler();
    }
    this.roleScheduler = scheduler;
    return this;
  }

  @Override
  public YarnScheduler getRoleScheduler() {
    if (roleScheduler == null) {
      ResourceProto.RoleSpecOrBuilder p = viaProto ? proto : builder;
      roleScheduler = p.getRoleScheduler();
    }
    return roleScheduler;
  }

  @Override
  public ElasticResource getElasticResource() {
    return getScheduleStrategy().getElasticResource();
  }
}
