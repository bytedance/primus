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
import com.bytedance.primus.apiserver.proto.DataProto.DataSavepoint;
import com.bytedance.primus.apiserver.records.DataSpec;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DataSpecImpl implements DataSpec {

  private DataProto.DataSpec proto = DataProto.DataSpec.getDefaultInstance();
  private DataProto.DataSpec.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, DataStreamSpec> dataStreamSpecs;
  private int totalDatastreamCount = -1;

  public DataSpecImpl() {
    builder = DataProto.DataSpec.newBuilder();
  }

  public DataSpecImpl(DataProto.DataSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized DataSpec setDataStreamSpecs(Map<String, DataStreamSpec> dataStreamSpecs) {
    maybeInitBuilder();
    if (dataStreamSpecs == null) {
      builder.clearDataStreamSpecs();
    }
    this.dataStreamSpecs = dataStreamSpecs;
    return this;
  }

  @Override
  public synchronized Map<String, DataStreamSpec> getDataStreamSpecs() {
    if (dataStreamSpecs != null) {
      return dataStreamSpecs;
    }
    DataProto.DataSpecOrBuilder p = viaProto ? proto : builder;
    dataStreamSpecs = new HashMap<>(p.getDataStreamSpecsMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new DataStreamSpecImpl(e.getValue()))));
    return dataStreamSpecs;
  }

  @Override
  public synchronized DataSpec setDataSavepoint(DataSavepoint dataSavepoint) {
    maybeInitBuilder();
    builder.setDataSavepoint(dataSavepoint);
    return this;
  }

  @Override
  public synchronized DataSavepoint getDataSavepoint() {
    DataProto.DataSpecOrBuilder p = viaProto ? proto : builder;
    return p.getDataSavepoint();
  }

  @Override
  public synchronized DataProto.DataSpec getProto() {
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
    if (!(obj instanceof DataSpecImpl)) {
      return false;
    }

    DataSpecImpl other = (DataSpecImpl) obj;
    boolean result = true;
    result = result && (getDataStreamSpecs().equals(other.getDataStreamSpecs()));
    result = result && (getDataSavepoint().equals(other.getDataSavepoint()));
    return result;
  }

  @Override
  public synchronized int getTotalDatastreamCount() {
    if (totalDatastreamCount != -1) {
      return totalDatastreamCount;
    }
    DataProto.DataSpecOrBuilder p = viaProto ? proto : builder;
    return p.getTotalDatastreamCount();
  }

  @Override
  public synchronized DataSpec setTotalDatastreamCount(int totalDatastreamCount) {
    maybeInitBuilder();
    builder.setTotalDatastreamCount(totalDatastreamCount);
    this.totalDatastreamCount = totalDatastreamCount;
    return this;
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
      builder = DataProto.DataSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (dataStreamSpecs != null) {
      addDataStreamSpecsToProto();
    }
  }

  private synchronized void addDataStreamSpecsToProto() {
    maybeInitBuilder();
    builder.clearDataStreamSpecs();
    if (dataStreamSpecs == null) {
      return;
    }
    builder.putAllDataStreamSpecs(
        dataStreamSpecs.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getProto()))
    );
  }
}
