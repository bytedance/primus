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

import com.bytedance.primus.api.records.Endpoint;
import com.bytedance.primus.proto.Primus;

public class EndpointPBImpl implements Endpoint {
  Primus.EndpointProto proto = Primus.EndpointProto.getDefaultInstance();
  Primus.EndpointProto.Builder builder = null;
  boolean viaProto = false;

  public EndpointPBImpl() {
    builder = Primus.EndpointProto.newBuilder();
  }

  public EndpointPBImpl(Primus.EndpointProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public Primus.EndpointProto getProto() {
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
      builder = Primus.EndpointProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getHostname() {
    Primus.EndpointProtoOrBuilder p = viaProto ? proto : builder;
    return p.getHostname();
  }

  @Override
  public void setHostname(String hostname) {
    maybeInitBuilder();
    builder.setHostname(hostname);
  }

  @Override
  public int getPort() {
    Primus.EndpointProtoOrBuilder p = viaProto ? proto : builder;
    return p.getPort();
  }

  @Override
  public void setPort(int port) {
    maybeInitBuilder();
    builder.setPort(port);
  }
}
