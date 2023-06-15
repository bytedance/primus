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
import com.bytedance.primus.apiserver.proto.DataProto.DataStreamStatus.DataStreamState;
import com.bytedance.primus.apiserver.records.DataSourceStatus;
import com.bytedance.primus.apiserver.records.DataStreamStatus;
import java.util.List;
import java.util.stream.Collectors;

public class DataStreamStatusImpl implements DataStreamStatus {

  private DataProto.DataStreamStatus proto = DataProto.DataStreamStatus.getDefaultInstance();
  private DataProto.DataStreamStatus.Builder builder = null;
  private boolean viaProto = false;

  private List<DataSourceStatus> dataSourceStatuses;

  public DataStreamStatusImpl() {
    builder = DataProto.DataStreamStatus.newBuilder();
  }

  public DataStreamStatusImpl(DataProto.DataStreamStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized DataStreamStatus setState(DataStreamState state) {
    maybeInitBuilder();
    builder.setState(state);
    return this;
  }

  @Override
  public synchronized DataStreamState getState() {
    DataProto.DataStreamStatusOrBuilder p = viaProto ? proto : builder;
    return p.getState();
  }

  @Override
  public synchronized DataStreamStatus setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress(progress);
    return this;
  }

  @Override
  public synchronized float getProgress() {
    DataProto.DataStreamStatusOrBuilder p = viaProto ? proto : builder;
    return p.getProgress();
  }

  @Override
  public synchronized DataStreamStatus setDataSourceStatuses(List<DataSourceStatus> dataSourceStatuses) {
    maybeInitBuilder();
    if (dataSourceStatuses == null) {
      builder.clearDataSourceStatuses();
    }
    this.dataSourceStatuses = dataSourceStatuses;
    return this;
  }

  @Override
  public synchronized List<DataSourceStatus> getDataSourceStatuses() {
    if (dataSourceStatuses != null) {
      return dataSourceStatuses;
    }
    DataProto.DataStreamStatusOrBuilder p = viaProto ? proto : builder;
    dataSourceStatuses = p.getDataSourceStatusesList().stream()
        .map(e -> new DataSourceStatusImpl(e)).collect(Collectors.toList());
    return dataSourceStatuses;
  }

  @Override
  public synchronized DataProto.DataStreamStatus getProto() {
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
    if (!(obj instanceof DataStreamStatusImpl)) {
      return false;
    }

    DataStreamStatusImpl other = (DataStreamStatusImpl) obj;
    boolean result = getState().equals(other.getState());
    result = result && getProgress() == other.getProgress();
    result = result && getDataSourceStatuses().equals(other.getDataSourceStatuses());
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
      builder = DataProto.DataStreamStatus.newBuilder(proto);
    }
    viaProto = false;
  }


  private synchronized void mergeLocalToBuilder() {
    if (dataSourceStatuses != null) {
      addDataSourceStatusesToProto();
    }
  }

  private synchronized void addDataSourceStatusesToProto() {
    maybeInitBuilder();
    builder.clearDataSourceStatuses();
    if (dataSourceStatuses == null) {
      return;
    }
    builder.addAllDataSourceStatuses(
        dataSourceStatuses.stream().map(e -> e.getProto()).collect(Collectors.toList()));
  }
}
