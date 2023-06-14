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
import com.bytedance.primus.apiserver.records.OwnerRef;
import java.util.List;
import java.util.stream.Collectors;

public class MetaImpl implements Meta {

  private ResourceServiceProto.Meta proto = ResourceServiceProto.Meta.getDefaultInstance();
  private ResourceServiceProto.Meta.Builder builder = null;
  private boolean viaProto = false;

  private List<OwnerRef> ownerRefs;

  public MetaImpl() {
    builder = ResourceServiceProto.Meta.newBuilder();
  }

  public MetaImpl(ResourceServiceProto.Meta proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized Meta setName(String name) {
    maybeInitBuilder();
    builder.setName(name);
    return this;
  }

  @Override
  public synchronized String getName() {
    ResourceServiceProto.MetaOrBuilder p = viaProto ? proto : builder;
    return p.getName();
  }

  @Override
  public synchronized Meta setVersion(long version) {
    maybeInitBuilder();
    builder.setVersion(version);
    return this;
  }

  @Override
  public synchronized long getVersion() {
    ResourceServiceProto.MetaOrBuilder p = viaProto ? proto : builder;
    return p.getVersion();
  }

  @Override
  public synchronized Meta setCreationTime(long creationTime) {
    maybeInitBuilder();
    builder.setCreationTime(creationTime);
    return this;
  }

  @Override
  public synchronized long getCreationTime() {
    ResourceServiceProto.MetaOrBuilder p = viaProto ? proto : builder;
    return p.getCreationTime();
  }

  @Override
  public synchronized Meta setDeletionTime(long deletionTime) {
    maybeInitBuilder();
    builder.setDeletionTime(deletionTime);
    return this;
  }

  @Override
  public synchronized long getDeletionTime() {
    ResourceServiceProto.MetaOrBuilder p = viaProto ? proto : builder;
    return p.getDeletionTime();
  }

  @Override
  public synchronized Meta setOwnerRefs(List<OwnerRef> ownerRefs) {
    maybeInitBuilder();
    if (ownerRefs == null) {
      builder.clearOwnerRefs();
    }
    this.ownerRefs = ownerRefs;
    return this;
  }

  @Override
  public synchronized List<OwnerRef> getOwnerRefs() {
    if (ownerRefs != null) {
      return ownerRefs;
    }
    ResourceServiceProto.MetaOrBuilder p = viaProto ? proto : builder;
    ownerRefs =
        p.getOwnerRefsList().stream().map(e -> new OwnerRefImpl(e)).collect(Collectors.toList());
    return ownerRefs;
  }

  @Override
  public synchronized ResourceServiceProto.Meta getProto() {
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
    if (!(obj instanceof MetaImpl)) {
      return false;
    }

    MetaImpl other = (MetaImpl) obj;
    boolean result = true;
    result = result && (getName().equals(other.getName()));
    result = result && (getVersion() == other.getVersion());
    result = result && (getCreationTime() == other.getCreationTime());
    result = result && (getDeletionTime() == other.getDeletionTime());
    result = result && (getOwnerRefs().equals(other.getOwnerRefs()));
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
      builder = ResourceServiceProto.Meta.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (ownerRefs != null) {
      addOwnerRefsToProto();
    }
  }

  private synchronized void addOwnerRefsToProto() {
    maybeInitBuilder();
    builder.clearOwnerRefs();
    if (ownerRefs == null) {
      return;
    }
    builder.addAllOwnerRefs(ownerRefs.stream().map(e -> e.getProto()).collect(Collectors.toList()));
  }
}
