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

package com.bytedance.primus.api.protocolrecords.impl.pb;

import com.bytedance.primus.api.protocolrecords.RegisterRequest;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorSpecPBImpl;
import com.bytedance.primus.proto.Primus;
import com.bytedance.primus.proto.Primus.RegisterRequestProto;
import com.bytedance.primus.proto.Primus.RegisterRequestProtoOrBuilder;

public class RegisterRequestPBImpl implements RegisterRequest {

  RegisterRequestProto proto =
      RegisterRequestProto.getDefaultInstance();
  RegisterRequestProto.Builder builder = null;
  boolean viaProto = false;

  public RegisterRequestPBImpl() {
    builder = RegisterRequestProto.newBuilder();
  }

  public RegisterRequestPBImpl(RegisterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public RegisterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ExecutorId getExecutorId() {
    RegisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasExecutorId()) ? convertFromProtoFormat(p.getExecutorId()) : null;
  }

  @Override
  public RegisterRequest setExecutorId(ExecutorId executorId) {
    maybeInitBuilder();
    builder.setExecutorId(convertToProtoFormat(executorId));
    return this;
  }

  @Override
  public ExecutorSpec getExecutorSpec() {
    RegisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasExecutorSpec()) ? convertFromProtoFormat(p.getExecutorSpec()) : null;
  }

  @Override
  public RegisterRequest setExecutorSpec(ExecutorSpec executorSpec) {
    maybeInitBuilder();
    builder.setExecutorSpec(convertToProtoFormat(executorSpec));
    return this;
  }

  private ExecutorIdPBImpl convertFromProtoFormat(Primus.ExecutorIdProto p) {
    return new ExecutorIdPBImpl(p);
  }

  private Primus.ExecutorIdProto convertToProtoFormat(ExecutorId t) {
    return ((ExecutorIdPBImpl) t).getProto();
  }

  private ExecutorSpecPBImpl convertFromProtoFormat(Primus.ExecutorSpecProto p) {
    return new ExecutorSpecPBImpl(p);
  }

  private Primus.ExecutorSpecProto convertToProtoFormat(ExecutorSpec t) {
    return ((ExecutorSpecPBImpl) t).getProto();
  }
}
