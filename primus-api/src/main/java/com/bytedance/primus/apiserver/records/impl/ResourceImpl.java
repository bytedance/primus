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
import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.Resource;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

public class ResourceImpl implements Resource {

  private ResourceServiceProto.Resource proto = ResourceServiceProto.Resource.getDefaultInstance();
  private ResourceServiceProto.Resource.Builder builder = null;
  private boolean viaProto = false;

  private Meta meta;

  public ResourceImpl() {
    builder = ResourceServiceProto.Resource.newBuilder();
  }

  public ResourceImpl(byte[] protoBytes) throws InvalidProtocolBufferException {
    this.proto = ResourceServiceProto.Resource.parseFrom(protoBytes);
    viaProto = true;
  }

  public ResourceImpl(ResourceServiceProto.Resource proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized Resource setKind(String kind) {
    maybeInitBuilder();
    builder.setKind(kind);
    return this;
  }

  @Override
  public synchronized String getKind() {
    ResourceServiceProto.ResourceOrBuilder p = viaProto ? proto : builder;
    return p.getKind();
  }

  @Override
  public synchronized Resource setMeta(Meta meta) {
    maybeInitBuilder();
    if (meta == null) {
      builder.clearMeta();
    }
    this.meta = meta;
    return this;
  }

  @Override
  public synchronized Meta getMeta() {
    if (meta != null) {
      return meta;
    }
    ResourceServiceProto.ResourceOrBuilder p = viaProto ? proto : builder;
    meta = new MetaImpl(p.getMeta());
    return meta;
  }

  @Override
  public synchronized Resource setSpec(Any spec) {
    maybeInitBuilder();
    builder.setSpec(spec);
    return this;
  }

  @Override
  public synchronized Any getSpec() {
    ResourceServiceProto.ResourceOrBuilder p = viaProto ? proto : builder;
    return p.getSpec();
  }

  @Override
  public synchronized Resource setStatus(Any status) {
    maybeInitBuilder();
    builder.setStatus(status);
    return this;
  }

  @Override
  public synchronized Any getStatus() {
    ResourceServiceProto.ResourceOrBuilder p = viaProto ? proto : builder;
    return p.getStatus();
  }

  @Override
  public synchronized ResourceServiceProto.Resource getProto() {
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
    if (!(obj instanceof ResourceImpl)) {
      return false;
    }

    ResourceImpl other = (ResourceImpl) obj;
    boolean result = true;
    result = result && (getKind().equals(other.getKind()));
    result = result && (getMeta().equals(other.getMeta()));
    result = result && (getSpec().equals(other.getSpec()));
    result = result && (getStatus().equals(other.getStatus()));
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
      builder = ResourceServiceProto.Resource.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (meta != null) {
      builder.setMeta(meta.getProto());
    }
  }
}
