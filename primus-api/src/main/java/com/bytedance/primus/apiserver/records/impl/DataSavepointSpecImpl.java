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
import com.bytedance.primus.apiserver.records.DataSavepointSpec;

public class DataSavepointSpecImpl implements DataSavepointSpec {

  private DataProto.DataSavepointSpec proto = DataProto.DataSavepointSpec.getDefaultInstance();
  private DataProto.DataSavepointSpec.Builder builder = null;
  private boolean viaProto = false;

  public DataSavepointSpecImpl() {
    builder = DataProto.DataSavepointSpec.newBuilder();
  }

  public DataSavepointSpecImpl(DataProto.DataSavepointSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public DataSavepointSpec setSavepointDir(String savepointDir) {
    maybeInitBuilder();
    builder.setSavepointDir(savepointDir);
    return this;
  }

  @Override
  public String getSavepointDir() {
    DataProto.DataSavepointSpecOrBuilder p = viaProto ? proto : builder;
    return p.getSavepointDir();
  }

  @Override
  public synchronized DataProto.DataSavepointSpec getProto() {
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
    if (!(obj instanceof DataSavepointSpecImpl)) {
      return false;
    }

    DataSavepointSpecImpl other = (DataSavepointSpecImpl) obj;
    return getSavepointDir().equals(other.getSavepointDir());
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
      builder = DataProto.DataSavepointSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
  }
}
