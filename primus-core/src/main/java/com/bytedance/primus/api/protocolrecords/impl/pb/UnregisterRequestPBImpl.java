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

import com.bytedance.primus.api.protocolrecords.UnregisterRequest;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.proto.Primus;
import com.bytedance.primus.proto.Primus.UnregisterRequestProto;
import com.bytedance.primus.proto.Primus.UnregisterRequestProtoOrBuilder;

public class UnregisterRequestPBImpl implements UnregisterRequest {

  UnregisterRequestProto proto =
      UnregisterRequestProto.getDefaultInstance();
  UnregisterRequestProto.Builder builder = null;
  boolean viaProto = false;

  public UnregisterRequestPBImpl() {
    builder = UnregisterRequestProto.newBuilder();
  }

  public UnregisterRequestPBImpl(UnregisterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public UnregisterRequestProto getProto() {
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
      builder = UnregisterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ExecutorId getExecutorId() {
    UnregisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasExecutorId()) ? convertFromProtoFormat(p.getExecutorId()) : null;
  }

  @Override
  public UnregisterRequest setExecutorId(ExecutorId executorId) {
    maybeInitBuilder();
    builder.setExecutorId(convertToProtoFormat(executorId));
    return this;
  }

  @Override
  public int getExitCode() {
    UnregisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getExitCode();
  }

  @Override
  public UnregisterRequest setExitCode(int exitCode) {
    maybeInitBuilder();
    builder.setExitCode(exitCode);
    return this;
  }

  @Override
  public String getFailMsg() {
    UnregisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFailMsg();
  }

  @Override
  public UnregisterRequest setFailMsg(String failMsg) {
    maybeInitBuilder();
    builder.setFailMsg(failMsg);
    return this;
  }

  private ExecutorIdPBImpl convertFromProtoFormat(Primus.ExecutorIdProto p) {
    return new ExecutorIdPBImpl(p);
  }

  private Primus.ExecutorIdProto convertToProtoFormat(ExecutorId t) {
    return ((ExecutorIdPBImpl) t).getProto();
  }
}
