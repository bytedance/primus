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
import com.bytedance.primus.apiserver.records.DataStatus;
import com.bytedance.primus.apiserver.records.DataStreamStatus;
import java.util.Map;
import java.util.stream.Collectors;

public class DataStatusImpl implements DataStatus {

  private DataProto.DataStatus proto = DataProto.DataStatus.getDefaultInstance();
  private DataProto.DataStatus.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, DataStreamStatus> dataStreamStatuses;

  public DataStatusImpl() {
    builder = DataProto.DataStatus.newBuilder();
  }

  public DataStatusImpl(DataProto.DataStatus proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized DataStatus setDataStreamStatuses(Map<String, DataStreamStatus> dataStreamStatuses) {
    maybeInitBuilder();
    if (dataStreamStatuses == null) {
      builder.clearDataStreamStatuses();
    }
    this.dataStreamStatuses = dataStreamStatuses;
    return this;
  }

  @Override
  public synchronized Map<String, DataStreamStatus> getDataStreamStatuses() {
    if (dataStreamStatuses != null) {
      return dataStreamStatuses;
    }
    DataProto.DataStatusOrBuilder p = viaProto ? proto : builder;
    dataStreamStatuses = p.getDataStreamStatusesMap().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> new DataStreamStatusImpl(e.getValue()))
    );
    return dataStreamStatuses;
  }

  @Override
  public synchronized DataProto.DataStatus getProto() {
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
    if (!(obj instanceof DataStatusImpl)) {
      return false;
    }

    DataStatusImpl other = (DataStatusImpl) obj;
    boolean result = true;
    result = result && (getDataStreamStatuses().equals(other.getDataStreamStatuses()));
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
      builder = DataProto.DataStatus.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    if (dataStreamStatuses != null) {
      addDataStreamStatusesToProto();
    }
  }

  private synchronized void addDataStreamStatusesToProto() {
    maybeInitBuilder();
    if (dataStreamStatuses == null) {
      return;
    }
    builder.putAllDataStreamStatuses(
        dataStreamStatuses.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getProto())
        )
    );
  }
}
