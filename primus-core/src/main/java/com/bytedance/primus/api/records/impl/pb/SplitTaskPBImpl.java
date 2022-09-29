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

import com.bytedance.primus.api.records.SplitTask;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputType;
import com.bytedance.primus.proto.Primus.TaskProto.SplitTaskProto;
import com.bytedance.primus.proto.Primus.TaskProto.SplitTaskProtoOrBuilder;

public class SplitTaskPBImpl implements SplitTask {

  SplitTaskProto proto = SplitTaskProto.getDefaultInstance();
  SplitTaskProto.Builder builder = null;
  boolean viaProto = false;

  public SplitTaskPBImpl() {
    builder = SplitTaskProto.newBuilder();
  }

  public SplitTaskPBImpl(SplitTaskProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SplitTaskProto getProto() {
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
      builder = SplitTaskProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getKey() {
    SplitTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getKey();
  }

  @Override
  public void setKey(String key) {
    maybeInitBuilder();
    builder.setKey(key);
  }

  @Override
  public String getPath() {
    SplitTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getPath();
  }

  @Override
  public void setPath(String path) {
    maybeInitBuilder();
    builder.setPath(path);
  }

  @Override
  public long getStart() {
    SplitTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStart();
  }

  @Override
  public void setStart(long start) {
    maybeInitBuilder();
    builder.setStart(start);
  }

  @Override
  public long getLength() {
    SplitTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLength();
  }

  @Override
  public void setLength(long length) {
    maybeInitBuilder();
    builder.setLength(length);
  }

  @Override
  public InputType getInputType() {
    SplitTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getInputType();
  }

  @Override
  public void setInputType(InputType inputType) {
    maybeInitBuilder();
    builder.setInputType(inputType);
  }

  @Override
  public String getTable() {
    SplitTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTable();
  }

  @Override
  public void setTable(String table) {
    maybeInitBuilder();
    if (table == null) {
      builder.clearTable();
    } else {
      builder.setTable(table);
    }
  }
}
