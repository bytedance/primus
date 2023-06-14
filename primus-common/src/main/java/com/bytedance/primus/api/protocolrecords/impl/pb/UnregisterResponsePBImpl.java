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

package com.bytedance.primus.api.protocolrecords.impl.pb;

import com.bytedance.primus.api.protocolrecords.UnregisterResponse;
import com.bytedance.primus.proto.Primus.UnregisterResponseProto;

public class UnregisterResponsePBImpl implements UnregisterResponse {

  UnregisterResponseProto proto =
      UnregisterResponseProto.getDefaultInstance();
  UnregisterResponseProto.Builder builder = null;
  boolean viaProto = false;

  public UnregisterResponsePBImpl() {
    builder = UnregisterResponseProto.newBuilder();
  }

  public UnregisterResponsePBImpl(UnregisterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UnregisterResponseProto getProto() {
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
      builder = UnregisterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

}
