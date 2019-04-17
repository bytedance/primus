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
import com.bytedance.primus.apiserver.records.RoleStatus;

public class RoleStatusImpl implements RoleStatus {

  private ResourceProto.RoleStatus proto = ResourceProto.RoleStatus.getDefaultInstance();
  private ResourceProto.RoleStatus.Builder builder = null;
  private boolean viaProto = false;

  public RoleStatusImpl() {
    builder = ResourceProto.RoleStatus.newBuilder();
  }

  public RoleStatusImpl(ResourceProto.RoleStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized RoleStatus setActiveNum(int activeNum) {
    maybeInitBuilder();
    builder.setActiveNum(activeNum);
    return this;
  }

  @Override
  public synchronized int getActiveNum() {
    ResourceProto.RoleStatusOrBuilder p = viaProto ? proto : builder;
    return p.getActiveNum();
  }

  @Override
  public synchronized RoleStatus setSucceedNum(int succeedNum) {
    maybeInitBuilder();
    builder.setSucceedNum(succeedNum);
    return this;
  }

  @Override
  public synchronized int getSucceedNum() {
    ResourceProto.RoleStatusOrBuilder p = viaProto ? proto : builder;
    return p.getSucceedNum();
  }

  @Override
  public synchronized RoleStatus setFailedNum(int failedNum) {
    maybeInitBuilder();
    builder.setFailedNum(failedNum);
    return this;
  }

  @Override
  public synchronized int getFailedNum() {
    ResourceProto.RoleStatusOrBuilder p = viaProto ? proto : builder;
    return p.getFailedNum();
  }

  @Override
  public synchronized ResourceProto.RoleStatus getProto() {
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
    if (!(obj instanceof RoleStatusImpl)) {
      return false;
    }

    RoleStatusImpl other = (RoleStatusImpl) obj;
    boolean result = true;
    result = result && (getActiveNum() == other.getActiveNum());
    result = result && (getSucceedNum() == other.getSucceedNum());
    result = result && (getFailedNum() == other.getFailedNum());
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
      builder = ResourceProto.RoleStatus.newBuilder(proto);
    }
    viaProto = false;
  }
}
