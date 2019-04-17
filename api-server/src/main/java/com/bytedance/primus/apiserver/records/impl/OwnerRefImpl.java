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

import com.bytedance.primus.apiserver.proto.ResourceServiceProto;
import com.bytedance.primus.apiserver.records.OwnerRef;

public class OwnerRefImpl implements OwnerRef {

  private ResourceServiceProto.OwnerRef proto = ResourceServiceProto.OwnerRef.getDefaultInstance();
  private ResourceServiceProto.OwnerRef.Builder builder = null;
  private boolean viaProto = false;

  public OwnerRefImpl() {
    builder = ResourceServiceProto.OwnerRef.newBuilder();
  }

  public OwnerRefImpl(ResourceServiceProto.OwnerRef proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized OwnerRef setName(String name) {
    maybeInitBuilder();
    builder.setName(name);
    return this;
  }

  @Override
  public synchronized String getName() {
    ResourceServiceProto.OwnerRefOrBuilder p = viaProto ? proto : builder;
    return p.getName();
  }

  @Override
  public synchronized OwnerRef setKind(String kind) {
    maybeInitBuilder();
    builder.setKind(kind);
    return this;
  }

  @Override
  public synchronized String getKind() {
    ResourceServiceProto.OwnerRefOrBuilder p = viaProto ? proto : builder;
    return p.getKind();
  }

  @Override
  public synchronized OwnerRef setUid(long uid) {
    maybeInitBuilder();
    builder.setUid(uid);
    return this;
  }

  @Override
  public synchronized long getUid() {
    ResourceServiceProto.OwnerRefOrBuilder p = viaProto ? proto : builder;
    return p.getUid();
  }

  @Override
  public synchronized ResourceServiceProto.OwnerRef getProto() {
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
    if (!(obj instanceof OwnerRefImpl)) {
      return false;
    }

    OwnerRefImpl other = (OwnerRefImpl) obj;
    boolean result = true;
    result = result && (getName().equals(other.getName()));
    result = result && (getKind().equals(other.getKind()));
    result = result && (getUid() == other.getUid());
    return result;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceServiceProto.OwnerRef.newBuilder(proto);
    }
    viaProto = false;
  }
}
