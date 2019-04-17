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

import com.bytedance.primus.apiserver.proto.DataProto;
import com.bytedance.primus.apiserver.proto.DataProto.DataSavepointStatus.DataSavepointState;
import com.bytedance.primus.apiserver.records.DataSavepointStatus;

public class DataSavepointStatusImpl implements DataSavepointStatus {

  private DataProto.DataSavepointStatus proto = DataProto.DataSavepointStatus
      .getDefaultInstance();
  private DataProto.DataSavepointStatus.Builder builder = null;
  private boolean viaProto = false;

  public DataSavepointStatusImpl() {
    builder = DataProto.DataSavepointStatus.newBuilder();
  }

  public DataSavepointStatusImpl(DataProto.DataSavepointStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public DataSavepointStatus setState(DataSavepointState state) {
    maybeInitBuilder();
    builder.setState(state);
    return this;
  }

  @Override
  public DataSavepointState getState() {
    DataProto.DataSavepointStatusOrBuilder p = viaProto ? proto : builder;
    return p.getState();
  }

  @Override
  public synchronized DataProto.DataSavepointStatus getProto() {
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
    if (!(obj instanceof DataSavepointStatusImpl)) {
      return false;
    }

    DataSavepointStatusImpl other = (DataSavepointStatusImpl) obj;
    return getState().equals(other.getState());
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
      builder = DataProto.DataSavepointStatus.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
  }
}
