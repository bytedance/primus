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

import com.bytedance.primus.api.records.FileTask;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.proto.PrimusTask.FileTaskProto;
import com.bytedance.primus.proto.PrimusTask.FileTaskProtoOrBuilder;

public class FileTaskPBImpl implements FileTask {

  FileTaskProto proto = FileTaskProto.getDefaultInstance();
  FileTaskProto.Builder builder = null;
  boolean viaProto = false;

  public FileTaskPBImpl(
      String path,
      long start,
      long length,
      String key,
      FileSourceSpec spec
  ) {
    builder = FileTaskProto
        .newBuilder()
        .setPath(path)
        .setStart(start)
        .setLength(length)
        .setBatchKey(key)
        .setSpec(spec);
  }

  public FileTaskPBImpl(FileTaskProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public FileTaskProto getProto() {
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
      builder = FileTaskProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getBatchKey() {
    FileTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getBatchKey();
  }

  @Override
  public String getPath() {
    FileTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getPath();
  }

  @Override
  public long getStart() {
    FileTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStart();
  }

  @Override
  public long getLength() {
    FileTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLength();
  }

  @Override
  public FileSourceSpec getSpec() {
    FileTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getSpec();
  }
}
