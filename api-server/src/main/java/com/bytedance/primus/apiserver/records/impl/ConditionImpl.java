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

import com.bytedance.primus.apiserver.proto.UtilsProto;
import com.bytedance.primus.apiserver.records.Condition;

public class ConditionImpl implements Condition {

  private UtilsProto.Condition proto = UtilsProto.Condition.getDefaultInstance();
  private UtilsProto.Condition.Builder builder = null;
  private boolean viaProto = false;

  public ConditionImpl() {
    builder = UtilsProto.Condition.newBuilder();
  }

  public ConditionImpl(UtilsProto.Condition proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized Condition setType(String type) {
    maybeInitBuilder();
    builder.setType(type);
    return this;
  }

  @Override
  public synchronized String getType() {
    UtilsProto.ConditionOrBuilder p = viaProto ? proto : builder;
    return p.getType();
  }

  @Override
  public synchronized Condition setReason(String reason) {
    maybeInitBuilder();
    builder.setReason(reason);
    return this;
  }

  @Override
  public synchronized String getReason() {
    UtilsProto.ConditionOrBuilder p = viaProto ? proto : builder;
    return p.getReason();
  }

  @Override
  public synchronized Condition setMessage(String message) {
    maybeInitBuilder();
    builder.setMessage(message);
    return this;
  }

  @Override
  public synchronized String getMessage() {
    UtilsProto.ConditionOrBuilder p = viaProto ? proto : builder;
    return p.getMessage();
  }

  @Override
  public synchronized Condition setLastTransitionTime(long lastTransitionTime) {
    maybeInitBuilder();
    builder.setLastTransitionTime(lastTransitionTime);
    return this;
  }

  @Override
  public synchronized long getLastTransitionTime() {
    UtilsProto.ConditionOrBuilder p = viaProto ? proto : builder;
    return p.getLastTransitionTime();
  }

  @Override
  public synchronized Condition setLastUpdateTime(long lastUpdateTime) {
    maybeInitBuilder();
    builder.setLastUpdateTime(lastUpdateTime);
    return this;
  }

  @Override
  public synchronized long getLastUpdateTime() {
    UtilsProto.ConditionOrBuilder p = viaProto ? proto : builder;
    return p.getLastUpdateTime();
  }

  @Override
  public synchronized UtilsProto.Condition getProto() {
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
    if (!(obj instanceof ConditionImpl)) {
      return false;
    }

    ConditionImpl other = (ConditionImpl) obj;
    boolean result = true;
    result = result && (getType().equals(other.getType()));
    result = result && (getReason().equals(other.getReason()));
    result = result && (getMessage().equals(other.getMessage()));
    result = result && (getLastTransitionTime() == other.getLastTransitionTime());
    result = result && (getLastUpdateTime() == other.getLastUpdateTime());
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
      builder = UtilsProto.Condition.newBuilder(proto);
    }
    viaProto = false;
  }
}
