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
import com.bytedance.primus.apiserver.records.NodeAttributeStatus;

public class NodeAttributeStatusImpl implements NodeAttributeStatus {

  private ResourceProto.NodeAttributeStatus proto =
      ResourceProto.NodeAttributeStatus.getDefaultInstance();
  private ResourceProto.NodeAttributeStatus.Builder builder = null;
  private boolean viaProto = false;

  private boolean isBlackListUpdated;

  public NodeAttributeStatusImpl() {
    builder = ResourceProto.NodeAttributeStatus.newBuilder();
  }

  public NodeAttributeStatusImpl(ResourceProto.NodeAttributeStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public boolean isBlackListUpdated() {
    return isBlackListUpdated;
  }

  @Override
  public void setBlackListUpdated(boolean blocked) {
    isBlackListUpdated = blocked;
  }

  @Override
  public ResourceProto.NodeAttributeStatus getProto() {
    mergeLocalToProto();
    ResourceProto.NodeAttributeStatus nodeAttributeStatus = viaProto ? proto : builder.build();
    viaProto = true;
    return nodeAttributeStatus;
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
      builder = ResourceProto.NodeAttributeStatus.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    builder.setIsBlackListUpdated(isBlackListUpdated);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof NodeAttributeStatusImpl)) {
      return false;
    }

    NodeAttributeStatusImpl other = (NodeAttributeStatusImpl) obj;
    boolean result = true;
    result = result && (isBlackListUpdated == other.isBlackListUpdated);
    return result;
  }
}
