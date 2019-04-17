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

import com.bytedance.primus.api.protocolrecords.RegisterResponse;
import com.bytedance.primus.api.records.ClusterSpec;
import com.bytedance.primus.api.records.ExecutorCommand;
import com.bytedance.primus.api.records.impl.pb.ClusterSpecPBImpl;
import com.bytedance.primus.api.records.impl.pb.ExecutorCommandPBImpl;
import com.bytedance.primus.proto.Primus;
import com.bytedance.primus.proto.Primus.RegisterResponseProto;
import com.bytedance.primus.proto.Primus.RegisterResponseProtoOrBuilder;

public class RegisterResponsePBImpl implements RegisterResponse {

  RegisterResponseProto proto = RegisterResponseProto.getDefaultInstance();
  RegisterResponseProto.Builder builder = null;
  boolean viaProto = false;

  public RegisterResponsePBImpl() {
    builder = RegisterResponseProto.newBuilder();
  }

  public RegisterResponsePBImpl(RegisterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public RegisterResponseProto getProto() {
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
      builder = RegisterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ClusterSpec getClusterSpec() {
    RegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasClusterSpec()) ? convertFromProtoFormat(p.getClusterSpec()) : null;
  }

  @Override
  public void setClusterSpec(ClusterSpec clusterSpec) {
    maybeInitBuilder();
    builder.setClusterSpec(convertToProtoFormat(clusterSpec));
  }

  @Override
  public ExecutorCommand getCommand() {
    RegisterResponseProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getCommand());
  }

  @Override
  public void setCommand(ExecutorCommand command) {
    maybeInitBuilder();
    builder.setCommand(convertToProtoFormat(command));
  }

  private ExecutorCommandPBImpl convertFromProtoFormat(
      RegisterResponseProto.ExecutorCommandProto p) {
    return new ExecutorCommandPBImpl(p);
  }

  private RegisterResponseProto.ExecutorCommandProto convertToProtoFormat(ExecutorCommand t) {
    return ((ExecutorCommandPBImpl) t).getProto();
  }

  private ClusterSpecPBImpl convertFromProtoFormat(Primus.ClusterSpecProto p) {
    return new ClusterSpecPBImpl(p);
  }

  private Primus.ClusterSpecProto convertToProtoFormat(ClusterSpec t) {
    return ((ClusterSpecPBImpl) t).getProto();
  }
}
