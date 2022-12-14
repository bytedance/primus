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
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.apiserver.proto.DataProto.KafkaSourceSpec;
import com.bytedance.primus.apiserver.records.DataSourceSpec;
import org.apache.commons.lang.StringUtils;

public class DataSourceSpecImpl implements DataSourceSpec {

  private DataProto.DataSourceSpec proto;
  private DataProto.DataSourceSpec.Builder builder = null;
  private boolean viaProto = false;

  private FileSourceSpec fileSourceSpec;
  private KafkaSourceSpec kafkaSourceSpec;

  public DataSourceSpecImpl() {
    builder = DataProto.DataSourceSpec.newBuilder();
  }

  public DataSourceSpecImpl(DataProto.DataSourceSpec proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized DataSourceSpec setSourceId(int sourceId) {
    maybeInitBuilder();
    builder.setSourceId(sourceId);
    return this;
  }

  @Override
  public synchronized int getSourceId() {
    DataProto.DataSourceSpecOrBuilder p = viaProto ? proto : builder;
    return p.getSourceId();
  }

  @Override
  public synchronized DataSourceSpec setSource(String source) {
    maybeInitBuilder();
    builder.setSource(source);
    return this;
  }

  @Override
  public synchronized String getSource() {
    DataProto.DataSourceSpecOrBuilder p = viaProto ? proto : builder;
    return p.getSource();
  }

  @Override
  public String getFileNameFilter() {
    DataProto.DataSourceSpecOrBuilder p = viaProto ? proto : builder;
    String filter = p.hasFileSourceSpec() ? p.getFileSourceSpec().getNamePattern() : "*";
    return StringUtils.isEmpty(filter) ? "*" : filter;
  }

  @Override
  public synchronized DataSourceSpec setFileSourceSpec(FileSourceSpec fileSourceSpec) {
    maybeInitBuilder();
    if (fileSourceSpec == null) {
      builder.clearFileSourceSpec();
    }
    this.fileSourceSpec = fileSourceSpec;
    return this;
  }

  @Override
  public synchronized FileSourceSpec getFileSourceSpec() {
    if (fileSourceSpec != null) {
      return fileSourceSpec;
    }
    DataProto.DataSourceSpecOrBuilder p = viaProto ? proto : builder;
    if (p.hasFileSourceSpec()) {
      fileSourceSpec = p.getFileSourceSpec();
    }
    return fileSourceSpec;
  }

  @Override
  public synchronized DataSourceSpec setKafkaSourceSpec(KafkaSourceSpec kafkaSourceSpec) {
    maybeInitBuilder();
    if (kafkaSourceSpec == null) {
      builder.clearKafkaSourceSpec();
    }
    this.kafkaSourceSpec = kafkaSourceSpec;
    return this;
  }

  @Override
  public synchronized KafkaSourceSpec getKafkaSourceSpec() {
    if (kafkaSourceSpec != null) {
      return kafkaSourceSpec;
    }
    DataProto.DataSourceSpecOrBuilder p = viaProto ? proto : builder;
    if (p.hasKafkaSourceSpec()) {
      kafkaSourceSpec = p.getKafkaSourceSpec();
    }
    return kafkaSourceSpec;
  }

  @Override
  public synchronized DataProto.DataSourceSpec getProto() {
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
    if (!(obj instanceof DataSourceSpecImpl)) {
      return false;
    }

    DataSourceSpecImpl other = (DataSourceSpecImpl) obj;
    boolean result = getSourceId() == other.getSourceId();
    result = result && (getSource().equals(other.getSource()));
    result = result && compare(getFileSourceSpec(), (other.getFileSourceSpec()));
    result = result && compare(getKafkaSourceSpec(), other.getKafkaSourceSpec());
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
      builder = DataProto.DataSourceSpec.newBuilder(proto);
    }
    viaProto = false;
  }

  private synchronized void mergeLocalToBuilder() {
    // merge oneof fields of proto
    if (fileSourceSpec != null) {
      builder.setFileSourceSpec(fileSourceSpec);
    } else if (kafkaSourceSpec != null) {
      builder.setKafkaSourceSpec(kafkaSourceSpec);
    }
  }

  private boolean compare(Object lhs, Object rhs) {
    if (lhs == null && rhs == null) {
      return true;
    } else if (lhs != null && rhs != null) {
      return lhs.equals(rhs);
    }
    return false;
  }
}
