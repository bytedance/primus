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
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.proto.Primus;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorSpecPBImpl implements ExecutorSpec {

  private static final Logger log = LoggerFactory.getLogger(ExecutorSpecPBImpl.class);

  Primus.ExecutorSpecProto proto = Primus.ExecutorSpecProto.getDefaultInstance();
  Primus.ExecutorSpecProto.Builder builder = null;
  boolean viaProto = false;

  public ExecutorSpecPBImpl() {
    builder = Primus.ExecutorSpecProto.newBuilder();
  }

  public ExecutorSpecPBImpl(Primus.ExecutorSpecProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public Primus.ExecutorSpecProto getProto() {
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
      builder = Primus.ExecutorSpecProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ExecutorId getExecutorId() {
    Primus.ExecutorSpecProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasExecutorId()) ? convertFromProtoFormat(p.getExecutorId()) : null;
  }

  @Override
  public ExecutorSpec setExecutorId(ExecutorId executorId) {
    maybeInitBuilder();
    builder.setExecutorId(convertToProtoFormat(executorId));
    return this;
  }

  @Override
  public List<Endpoint> getEndpoints() {
    Primus.ExecutorSpecProtoOrBuilder p = viaProto ? proto : builder;
    List<Endpoint> endpoints = new ArrayList<>();
    for (Primus.EndpointProto endpoint : p.getEndpointsList()) {
      endpoints.add(convertFromProtoFormat(endpoint));
    }
    return endpoints;
  }

  @Override
  public ExecutorSpec setEndpoints(List<Endpoint> endpoints) {
    maybeInitBuilder();
    builder.clearEndpoints();
    builder.addAllEndpoints(() -> new Iterator<Primus.EndpointProto>() {
      Iterator<Endpoint> iter = endpoints.iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Primus.EndpointProto next() {
        return convertToProtoFormat(iter.next());
      }
    });
    return this;
  }

  private ExecutorIdPBImpl convertFromProtoFormat(Primus.ExecutorIdProto p) {
    return new ExecutorIdPBImpl(p);
  }

  private Primus.ExecutorIdProto convertToProtoFormat(ExecutorId t) {
    return ((ExecutorIdPBImpl) t).getProto();
  }

  private EndpointPBImpl convertFromProtoFormat(Primus.EndpointProto p) {
    return new EndpointPBImpl(p);
  }

  private Primus.EndpointProto convertToProtoFormat(Endpoint t) {
    return ((EndpointPBImpl) t).getProto();
  }

  @Override
  public String toString() {
    JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();
    try {
      return printer.print(getProto());
    } catch (InvalidProtocolBufferException e) {
      log.error("error happened when convert ExecutorSpecPBImpl to String", e);
    }
    return super.toString();
  }
}
