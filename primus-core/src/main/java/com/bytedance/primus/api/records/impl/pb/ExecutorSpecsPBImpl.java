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

import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.ExecutorSpecs;
import com.bytedance.primus.proto.Primus.ExecutorSpecsProto;
import com.bytedance.primus.proto.Primus.ExecutorSpecsProtoOrBuilder;
import com.bytedance.primus.proto.Primus.ExecutorSpecProto;
import java.util.List;
import java.util.stream.Collectors;

public class ExecutorSpecsPBImpl implements ExecutorSpecs {

  ExecutorSpecsProto proto = ExecutorSpecsProto.getDefaultInstance();
  ExecutorSpecsProto.Builder builder = null;
  boolean viaProto = false;

  private List<ExecutorSpec> executorSpecs;

  public ExecutorSpecsPBImpl() {
    builder = ExecutorSpecsProto.newBuilder();
  }

  public ExecutorSpecsPBImpl(ExecutorSpecsProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ExecutorSpecsProto getProto() {
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
      builder = ExecutorSpecsProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (executorSpecs != null) {
      addExecutorSpecToProto();
    }
  }

  @Override
  public List<ExecutorSpec> getExecutorSpecs() {
    if (executorSpecs != null) {
      return executorSpecs;
    }
    ExecutorSpecsProtoOrBuilder p = viaProto ? proto : builder;
    executorSpecs = p.getExecutorSpecsList().stream()
        .map(e -> convertFromProtoFormat(e)).collect(Collectors.toList());
    return executorSpecs;
  }

  @Override
  public void setExecutorSpecs(List<ExecutorSpec> executorSpecs) {
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
    builder.addAllExecutorSpecs(
        executorSpecs.stream().map(e -> convertToProtoFormat(e)).collect(Collectors.toList()));
  }

  private ExecutorSpecPBImpl convertFromProtoFormat(ExecutorSpecProto p) {
    return new ExecutorSpecPBImpl(p);
  }

  private ExecutorSpecProto convertToProtoFormat(ExecutorSpec t) {
    if (t == null) {
      return ExecutorSpecProto.getDefaultInstance();
    } else {
      return ((ExecutorSpecPBImpl) t).getProto();
    }
  }
}
