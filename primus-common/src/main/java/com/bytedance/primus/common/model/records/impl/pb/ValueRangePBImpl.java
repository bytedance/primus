/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */
package com.bytedance.primus.common.model.records.impl.pb;

import com.bytedance.primus.common.model.records.ValueRange;
import com.bytedance.primus.common.proto.ModelProtos.ValueRangeProto;
import com.bytedance.primus.common.proto.ModelProtos.ValueRangeProtoOrBuilder;

public class ValueRangePBImpl extends ValueRange {

  ValueRangeProto proto = ValueRangeProto.getDefaultInstance();
  ValueRangeProto.Builder builder = null;
  boolean viaProto = false;
  int begin, end = -1;

  public ValueRangePBImpl(ValueRangeProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ValueRangePBImpl() {
  }

  public ValueRangeProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int getBegin() {
    initLocalRange();
    return begin;
  }

  @Override
  public int getEnd() {
    initLocalRange();
    return end;
  }

  @Override
  public void setBegin(int value) {
    begin = value;
  }

  @Override
  public void setEnd(int value) {
    end = value;
  }

  @Override
  public boolean isLessOrEqual(ValueRange other) {
    if (this.getBegin() >= other.getBegin() && this.getEnd() <= other.getEnd()) {
      return true;
    }
    return false;
  }

  private void maybeInitBuilder() {
    if (viaProto) {
      builder = ValueRangeProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (begin != -1 && end != -1) {
      addRangeToProto();
    }
  }

  private void addRangeToProto() {
    maybeInitBuilder();
    if (begin == -1 && end == -1) {
      return;
    }
    if (builder == null) {
      builder = ValueRangeProto.newBuilder();
    }
    builder.setBegin(begin);
    builder.setEnd(end);
  }

  private void initLocalRange() {
    if (begin != -1 && end != -1) {
      return;
    }
    if (!viaProto && builder == null) {
      builder = ValueRangeProto.newBuilder();
    }
    ValueRangeProtoOrBuilder p = viaProto ? proto : builder;
    begin = p.getBegin();
    end = p.getEnd();
  }

}
