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

package com.bytedance.primus.api.records.impl.pb;

import com.bytedance.primus.api.records.ClusterSpec;
import com.bytedance.primus.api.records.ExecutorSpecs;
import com.bytedance.primus.proto.Primus.ClusterSpecProto;
import com.bytedance.primus.proto.Primus.ClusterSpecProtoOrBuilder;
import com.bytedance.primus.proto.Primus.ExecutorSpecsProto;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterSpecPBImpl implements ClusterSpec {

  ClusterSpecProto proto = ClusterSpecProto.getDefaultInstance();
  ClusterSpecProto.Builder builder = null;
  boolean viaProto = false;

  private Map<String, ExecutorSpecs> executorSpecs;

  public ClusterSpecPBImpl() {
    builder = ClusterSpecProto.newBuilder();
  }

  public ClusterSpecPBImpl(ClusterSpecProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ClusterSpecProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ClusterSpecProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (executorSpecs != null) {
      addExecutorSpecToProto();
    }
  }

  @Override
  public Map<String, ExecutorSpecs> getExecutorSpecs() {
    if (executorSpecs != null) {
      return executorSpecs;
    }
    ClusterSpecProtoOrBuilder p = viaProto ? proto : builder;
    executorSpecs = p.getExecutorSpecsMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> convertFromProtoFormat(e.getValue())));
    return executorSpecs;
  }

  @Override
  public void setExecutorSpecs(Map<String, ExecutorSpecs> executorSpecs) {
    maybeInitBuilder();
    if (executorSpecs == null) {
      builder.clearExecutorSpecs();
    }
    this.executorSpecs = executorSpecs;
  }

  private void addExecutorSpecToProto() {
    maybeInitBuilder();
    builder.clearExecutorSpecs();
    if (executorSpecs == null) {
      return;
    }
    builder.putAllExecutorSpecs(executorSpecs.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> convertToProtoFormat(e.getValue()))));
  }

  private ExecutorSpecsPBImpl convertFromProtoFormat(ExecutorSpecsProto p) {
    return new ExecutorSpecsPBImpl(p);
  }

  private ExecutorSpecsProto convertToProtoFormat(ExecutorSpecs t) {
    return ((ExecutorSpecsPBImpl) t).getProto();
  }
}
