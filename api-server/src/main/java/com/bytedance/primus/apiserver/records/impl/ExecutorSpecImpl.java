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
import com.bytedance.primus.apiserver.proto.ResourceProto.PluginConfig;
import com.bytedance.primus.apiserver.proto.UtilsProto.InputPolicy;
import com.bytedance.primus.apiserver.proto.UtilsProto.ResourceRequest;
import com.bytedance.primus.apiserver.records.ExecutorSpec;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecutorSpecImpl implements ExecutorSpec {

  private ResourceProto.ExecutorSpec proto = ResourceProto.ExecutorSpec.getDefaultInstance();
  private ResourceProto.ExecutorSpec.Builder builder = null;
  private boolean viaProto = false;

  private List<ResourceRequest> resourceRequests;
  private Map<String, String> envs;
  private InputPolicy inputPolicy;

  public ExecutorSpecImpl() {
    builder = ResourceProto.ExecutorSpec.newBuilder();
  }

  public ExecutorSpecImpl(ResourceProto.ExecutorSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized ExecutorSpec setRoleIndex(int roleIndex) {
    maybeInitBuilder();
    builder.setRoleIndex(roleIndex);
    return this;
  }

  @Override
  public synchronized int getRoleIndex() {
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    return p.getRoleIndex();
  }

  @Override
  public synchronized ExecutorSpec setResourceRequests(List<ResourceRequest> resourceRequests) {
    maybeInitBuilder();
    if (resourceRequests == null) {
      builder.clearResourceRequests();
    }
    this.resourceRequests = resourceRequests;
    return this;
  }

  @Override
  public synchronized List<ResourceRequest> getResourceRequests() {
    if (resourceRequests != null) {
      return resourceRequests;
    }
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    resourceRequests = new LinkedList<>(p.getResourceRequestsList());
    return resourceRequests;
  }

  @Override
  public synchronized ExecutorSpec setJavaOpts(String javaOpts) {
    maybeInitBuilder();
    builder.setJavaOpts(javaOpts);
    return this;
  }

  @Override
  public synchronized String getJavaOpts() {
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    return p.getJavaOpts();
  }

  @Override
  public synchronized ExecutorSpec setCommand(String command) {
    maybeInitBuilder();
    builder.setCommand(command);
    return this;
  }

  @Override
  public synchronized String getCommand() {
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    return p.getCommand();
  }

  @Override
  public synchronized ExecutorSpec setEnvs(Map<String, String> envs) {
    maybeInitBuilder();
    if (envs == null) {
      builder.clearEnvs();
    }
    this.envs = envs;
    return this;
  }

  @Override
  public synchronized Map<String, String> getEnvs() {
    if (envs != null) {
      return envs;
    }
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    envs = new HashMap<>(p.getEnvsMap());
    return envs;
  }

  @Override
  public synchronized ExecutorSpec setInputPolicy(InputPolicy inputPolicy) {
    maybeInitBuilder();
    if (inputPolicy == null) {
      builder.clearInputPolicy();
    }
    this.inputPolicy = inputPolicy;
    return this;
  }

  @Override
  public synchronized InputPolicy getInputPolicy() {
    if (inputPolicy != null) {
      return inputPolicy;
    }
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    inputPolicy = p.getInputPolicy();
    return inputPolicy;
  }

  @Override
  public synchronized ExecutorSpec setIsEvaluation(boolean isEvaluation) {
    maybeInitBuilder();
    builder.setIsEvaluation(isEvaluation);
    return this;
  }

  @Override
  public synchronized boolean getIsEvaluation() {
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    return p.getIsEvaluation();
  }

  @Override
  public PluginConfig getPluginConfig() {
    ResourceProto.ExecutorSpecOrBuilder p = viaProto ? proto : builder;
    return p.getPluginConfig();
  }

  @Override
  public synchronized ResourceProto.ExecutorSpec getProto() {
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
    if (!(obj instanceof ExecutorSpecImpl)) {
      return false;
    }

    ExecutorSpecImpl other = (ExecutorSpecImpl) obj;
    boolean result = true;
    result = result && (getRoleIndex() == other.getRoleIndex());
    result = result && (getResourceRequests().equals(other.getResourceRequests()));
    result = result && (getJavaOpts().equals(other.getJavaOpts()));
    result = result && (getCommand().equals(other.getCommand()));
    result = result && (getEnvs().equals(other.getEnvs()));
    result = result && (getInputPolicy().equals(other.getInputPolicy()));
    result = result && (getPluginConfig().equals(other.getPluginConfig()));
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
      builder = ResourceProto.ExecutorSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (resourceRequests != null) {
      addResourceRequestsToProto();
    }
    if (envs != null) {
      addEnvsToProto();
    }
    if (inputPolicy != null) {
      builder.setInputPolicy(inputPolicy);
    }
  }

  private synchronized void addResourceRequestsToProto() {
    maybeInitBuilder();
    builder.clearResourceRequests();
    if (resourceRequests == null) {
      return;
    }
    builder.addAllResourceRequests(resourceRequests);
  }

  private synchronized void addEnvsToProto() {
    maybeInitBuilder();
    builder.clearEnvs();
    if (envs == null) {
      return;
    }
    builder.putAllEnvs(envs.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> e.getValue())
    ));
  }
}
