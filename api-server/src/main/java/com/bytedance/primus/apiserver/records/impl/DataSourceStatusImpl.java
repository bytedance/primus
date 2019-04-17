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
import com.bytedance.primus.apiserver.records.DataSourceStatus;

public class DataSourceStatusImpl implements DataSourceStatus {

  private DataProto.DataSourceStatus proto = DataProto.DataSourceStatus.getDefaultInstance();
  private DataProto.DataSourceStatus.Builder builder = null;
  private boolean viaProto = false;

  public DataSourceStatusImpl() {
    builder = DataProto.DataSourceStatus.newBuilder();
  }

  public DataSourceStatusImpl(DataProto.DataSourceStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized DataSourceStatus setSourceId(String sourceId) {
    maybeInitBuilder();
    builder.setSourceId(sourceId);
    return this;
  }

  @Override
  public synchronized String getSourceId() {
    DataProto.DataSourceStatusOrBuilder p = viaProto ? proto : builder;
    return p.getSourceId();
  }

  @Override
  public synchronized DataSourceStatus setDataConsumptionTime(int time) {
    maybeInitBuilder();
    builder.setDataConsumptionTime(time);
    return this;
  }

  @Override
  public synchronized int getDataConsumptionTime() {
    DataProto.DataSourceStatusOrBuilder p = viaProto ? proto : builder;
    return p.getDataConsumptionTime();
  }

  @Override
  public synchronized DataProto.DataSourceStatus getProto() {
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
    if (!(obj instanceof DataSourceStatusImpl)) {
      return false;
    }

    DataSourceStatusImpl other = (DataSourceStatusImpl) obj;
    boolean result = getSourceId().equals(other.getSourceId());
    result = result && getDataConsumptionTime() == other.getDataConsumptionTime();
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
      builder = DataProto.DataSourceStatus.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
  }
}
