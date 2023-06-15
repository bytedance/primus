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
import com.bytedance.primus.apiserver.records.NodeAttributeSpec;

import java.util.Map;
import java.util.Set;

public class NodeAttributeSpecImpl implements NodeAttributeSpec {

  private ResourceProto.NodeAttributeSpec proto =
      ResourceProto.NodeAttributeSpec.getDefaultInstance();
  private ResourceProto.NodeAttributeSpec.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, Long> blackList;

  public NodeAttributeSpecImpl() {
    builder = ResourceProto.NodeAttributeSpec.newBuilder();
  }

  public NodeAttributeSpecImpl(ResourceProto.NodeAttributeSpec proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  @Override
  public NodeAttributeSpec setBlackList(Map<String, Long> blackList) {
    this.blackList = blackList;
    return this;
  }

  @Override
  public NodeAttributeSpec addBlackList(String k, long v) {
    this.blackList.put(k, v);
    return this;
  }

  @Override
  public NodeAttributeSpec deleteBlackList(String key) {
    this.blackList.remove(key);
    return this;
  }

  @Override
  public NodeAttributeSpec removeBlackList(Set<String> keys) {
    for (String key : keys) {
      this.blackList.remove(key);
    }
    return this;
  }

  @Override
  public NodeAttributeSpec clearBlackList() {
    this.blackList.clear();
    return this;
  }

  @Override
  public Map<String, Long> getBlackList() {
    return blackList;
  }

  @Override
  public ResourceProto.NodeAttributeSpec getProto() {
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
    if (!(obj instanceof NodeAttributeSpecImpl)) {
      return false;
    }

    NodeAttributeSpecImpl other = (NodeAttributeSpecImpl) obj;
    boolean result = true;
    result = result && (getBlackList().equals(other.getBlackList()));
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
      builder = ResourceProto.NodeAttributeSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (blackList != null) {
      addBlackListToProto();
    }
  }

  private void addBlackListToProto() {
    maybeInitBuilder();
    builder.clearBlackList();
    if (blackList == null) {
      return;
    }
    builder.putAllBlackList(blackList);
  }
}
