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
import com.bytedance.primus.apiserver.records.DataSourceSpec;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import java.util.List;
import java.util.stream.Collectors;

public class DataStreamSpecImpl implements DataStreamSpec {

  private DataProto.DataStreamSpec proto = DataProto.DataStreamSpec.getDefaultInstance();
  private DataProto.DataStreamSpec.Builder builder = null;
  private boolean viaProto = false;

  List<DataSourceSpec> dataSourceSpecs;
  private String workflowName;

  public DataStreamSpecImpl() {
    builder = DataProto.DataStreamSpec.newBuilder();
  }

  public DataStreamSpecImpl(DataProto.DataStreamSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized DataStreamSpec setDataSourceSpecs(List<DataSourceSpec> dataSourceSpecs) {
    maybeInitBuilder();
    if (dataSourceSpecs == null) {
      builder.clearDataSourceSpecs();
    }
    this.dataSourceSpecs = dataSourceSpecs;
    return this;
  }

  @Override
  public synchronized List<DataSourceSpec> getDataSourceSpecs() {
    if (dataSourceSpecs != null) {
      return dataSourceSpecs;
    }
    DataProto.DataStreamSpecOrBuilder p = viaProto ? proto : builder;
    dataSourceSpecs = p.getDataSourceSpecsList().stream().map(e -> new DataSourceSpecImpl(e))
        .collect(Collectors.toList());
    return dataSourceSpecs;
  }

  @Override
  public synchronized DataProto.DataStreamSpec getProto() {
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
    if (!(obj instanceof DataStreamSpecImpl)) {
      return false;
    }

    DataStreamSpecImpl other = (DataStreamSpecImpl) obj;
    boolean result = true;
    result = result && (getDataSourceSpecs().equals(other.getDataSourceSpecs()));
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
      builder = DataProto.DataStreamSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (dataSourceSpecs != null) {
      addDataSourcesToProto();
    }
  }

  private synchronized void addDataSourcesToProto() {
    maybeInitBuilder();
    builder.clearDataSourceSpecs();
    if (dataSourceSpecs == null) {
      return;
    }
    builder.addAllDataSourceSpecs(
        dataSourceSpecs.stream().map(e -> e.getProto()).collect(Collectors.toList()));
  }
}
