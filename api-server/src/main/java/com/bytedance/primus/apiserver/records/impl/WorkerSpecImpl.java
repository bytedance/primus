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
import com.bytedance.primus.apiserver.records.WorkerSpec;
import com.google.protobuf.Any;
import java.util.HashMap;
import java.util.Map;

public class WorkerSpecImpl implements WorkerSpec {

  private ResourceProto.WorkerSpec proto = ResourceProto.WorkerSpec.getDefaultInstance();
  private ResourceProto.WorkerSpec.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, Any> specs;

  public WorkerSpecImpl() {
    builder = ResourceProto.WorkerSpec.newBuilder();
  }

  public WorkerSpecImpl(ResourceProto.WorkerSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized WorkerSpec setSpecs(Map<String, Any> specs) {
    maybeInitBuilder();
    if (specs == null) {
      builder.clearSpecs();
    }
    this.specs = specs;
    return this;
  }

  @Override
  public synchronized Map<String, Any> getSpecs() {
    if (specs != null) {
      return specs;
    }
    ResourceProto.WorkerSpecOrBuilder p = viaProto ? proto : builder;
    specs = new HashMap<>(p.getSpecsMap());
    return specs;
  }

  @Override
  public synchronized ResourceProto.WorkerSpec getProto() {
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
    if (!(obj instanceof WorkerSpecImpl)) {
      return false;
    }

    WorkerSpecImpl other = (WorkerSpecImpl) obj;
    boolean result = true;
    result = result && (getSpecs().equals(other.getSpecs()));
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
      builder = ResourceProto.WorkerSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (specs != null) {
      addSpecsToProto();
    }
  }

  private synchronized void addSpecsToProto() {
    maybeInitBuilder();
    builder.clearSpecs();
    if (specs == null) {
      return;
    }
    builder.putAllSpecs(specs);
  }
}