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

import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.proto.ModelProtos.PriorityProto;
import com.bytedance.primus.common.proto.ModelProtos.PriorityProtoOrBuilder;


public class PriorityPBImpl extends Priority {

  PriorityProto proto = PriorityProto.getDefaultInstance();
  PriorityProto.Builder builder = null;
  boolean viaProto = false;

  public PriorityPBImpl() {
    builder = PriorityProto.newBuilder();
  }

  public PriorityPBImpl(PriorityProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public PriorityProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PriorityProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public int getPriority() {
    PriorityProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPriority());
  }

  @Override
  public void setPriority(int priority) {
    maybeInitBuilder();
    builder.setPriority((priority));
  }

  @Override
  public String toString() {
    return Integer.valueOf(getPriority()).toString();
  }

}  
