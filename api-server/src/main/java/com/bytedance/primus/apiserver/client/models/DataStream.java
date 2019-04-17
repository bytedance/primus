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

package com.bytedance.primus.apiserver.client.models;

import com.bytedance.primus.apiserver.proto.DataProto;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import com.bytedance.primus.apiserver.records.DataStreamStatus;
import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.records.impl.DataStreamSpecImpl;
import com.bytedance.primus.apiserver.records.impl.DataStreamStatusImpl;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.apiserver.service.exception.ErrorCode;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;

public class DataStream extends AbstractApiType<DataStream, DataStreamSpec, DataStreamStatus> {

  public static final String KIND = "DataStream";

  private Meta meta;
  private DataStreamSpec spec;
  private DataStreamStatus status;

  @Override
  public String getKind() {
    return KIND;
  }

  @Override
  public Meta getMeta() {
    return meta;
  }

  @Override
  public DataStream setMeta(Meta meta) {
    this.meta = meta;
    return this;
  }

  @Override
  public DataStreamSpec getSpec() {
    return spec;
  }

  @Override
  public DataStream setSpec(DataStreamSpec spec) {
    this.spec = spec;
    return this;
  }

  @Override
  public DataStreamStatus getStatus() {
    return status;
  }

  @Override
  public DataStream setStatus(DataStreamStatus status) {
    this.status = status;
    return this;
  }

  @Override
  public Resource toResource() {
    Any specAny = spec != null ? Any.pack(spec.getProto()) : null;
    Any statusAny = status != null ? Any.pack(status.getProto()) : null;
    return toResourceImpl(KIND, meta, specAny, statusAny);
  }

  @Override
  public DataStream fromResource(Resource resource) throws ApiServerException {
    if (resource.getKind().equals(this.KIND)) {
      try {
        meta = resource.getMeta();
        spec = new DataStreamSpecImpl(
            resource.getSpec().unpack(DataProto.DataStreamSpec.class));
        // ensure status has been set
        if (resource.getStatus().is(DataProto.DataStreamStatus.class)) {
          status =
              new DataStreamStatusImpl(
                  resource.getStatus().unpack(DataProto.DataStreamStatus.class));
        } else {
          status = new DataStreamStatusImpl();
        }
        return this;
      } catch (InvalidProtocolBufferException e) {
        throw new ApiServerException(e);
      }
    } else {
      throw new ApiServerException(ErrorCode.INVALID_ARGUMENT,
          "Incompatible resource kind, required " + this.KIND + ", provided" + resource.getKind());
    }
  }

  @Override
  public String toString() {
    MessageOrBuilder metaMsg = meta != null ? meta.getProto() : null;
    MessageOrBuilder specMsg = spec != null ? spec.getProto() : null;
    MessageOrBuilder statusMsg = status != null ? status.getProto() : null;
    return toStringImpl(KIND, metaMsg, specMsg, statusMsg);
  }
}
