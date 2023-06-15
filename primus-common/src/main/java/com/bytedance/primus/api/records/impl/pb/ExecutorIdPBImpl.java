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

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.proto.Primus.ExecutorIdProto;
import com.bytedance.primus.proto.Primus.ExecutorIdProtoOrBuilder;

public class ExecutorIdPBImpl implements ExecutorId {

  ExecutorIdProto proto = ExecutorIdProto.getDefaultInstance();
  ExecutorIdProto.Builder builder = null;
  boolean viaProto = false;

  public ExecutorIdPBImpl() {
    builder = ExecutorIdProto.newBuilder();
  }

  public ExecutorIdPBImpl(ExecutorIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ExecutorIdProto getProto() {
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
      builder = ExecutorIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getRoleName() {
    ExecutorIdProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRoleName();
  }

  @Override
  public void setRoleName(String roleName) {
    maybeInitBuilder();
    builder.setRoleName(roleName);
  }

  @Override
  public int getIndex() {
    ExecutorIdProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIndex();
  }

  @Override
  public void setIndex(int index) {
    maybeInitBuilder();
    builder.setIndex(index);
  }

  @Override
  public long getUniqId() {
    ExecutorIdProtoOrBuilder p = viaProto ? proto : builder;
    return p.getUniqId();
  }

  @Override
  public void setUniqId(long uniqId) {
    maybeInitBuilder();
    builder.setUniqId(uniqId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExecutorIdPBImpl)) {
      return false;
    }
    ExecutorIdPBImpl other = (ExecutorIdPBImpl)o;
    return this.getRoleName().equals(other.getRoleName()) && this.getIndex() == other.getIndex();
  }

  @java.lang.Override
  public int hashCode() {
    int hash = 41;
    hash = (53 * hash) + getRoleName().hashCode();
    hash = (53 * hash) + getIndex();
    return hash;
  }

  @Override
  public String toString() {
    return "executor_" + getRoleName() + "_" + getIndex();
  }

  @Override
  public String toUniqString() {
    return "executor_" + getRoleName() + "_" + getIndex() + "_" + getUniqId();
  }
}
