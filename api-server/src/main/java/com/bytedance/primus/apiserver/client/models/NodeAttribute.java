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

import com.bytedance.primus.apiserver.proto.ResourceProto;
import com.bytedance.primus.apiserver.records.Meta;
import com.bytedance.primus.apiserver.records.NodeAttributeSpec;
import com.bytedance.primus.apiserver.records.NodeAttributeStatus;
import com.bytedance.primus.apiserver.records.Resource;
import com.bytedance.primus.apiserver.records.impl.NodeAttributeSpecImpl;
import com.bytedance.primus.apiserver.records.impl.NodeAttributeStatusImpl;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.apiserver.service.exception.ErrorCode;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;

public class NodeAttribute
    extends AbstractApiType<NodeAttribute, NodeAttributeSpec, NodeAttributeStatus> {

  public static final String KIND = "NodeAttribute";

  private Meta meta;
  private NodeAttributeSpec spec;
  private NodeAttributeStatus status;

  @Override
  public String getKind() {
    return KIND;
  }

  @Override
  public Meta getMeta() {
    return meta;
  }

  @Override
  public NodeAttribute setMeta(Meta meta) {
    this.meta = meta;
    return this;
  }

  @Override
  public NodeAttributeSpec getSpec() {
    return spec;
  }

  @Override
  public NodeAttribute setSpec(NodeAttributeSpec nodeAttributeSpec) {
    this.spec = nodeAttributeSpec;
    return this;
  }

  @Override
  public NodeAttributeStatus getStatus() {
    return status;
  }

  @Override
  public NodeAttribute setStatus(NodeAttributeStatus nodeAttributeStatus) {
    this.status = nodeAttributeStatus;
    return this;
  }

  @Override
  public Resource toResource() {
    Any specAny = spec != null ? Any.pack(spec.getProto()) : null;
    Any statusAny = status != null ? Any.pack(status.getProto()) : null;
    return toResourceImpl(KIND, meta, specAny, statusAny);
  }

  @Override
  public NodeAttribute fromResource(Resource resource) throws ApiServerException {
    if (resource.getKind().equals(KIND)) {
      try {
        meta = resource.getMeta();
        spec =
            new NodeAttributeSpecImpl(
                resource.getSpec().unpack(ResourceProto.NodeAttributeSpec.class));
        // ensure status has been set
        if (resource.getStatus().is(ResourceProto.NodeAttributeStatus.class)) {
          status =
              new NodeAttributeStatusImpl(
                  resource.getStatus().unpack(ResourceProto.NodeAttributeStatus.class));
        } else {
          status = new NodeAttributeStatusImpl();
        }
        return this;
      } catch (InvalidProtocolBufferException e) {
        throw new ApiServerException(e);
      }
    } else {
      throw new ApiServerException(
          ErrorCode.INVALID_ARGUMENT,
          "Incompatible resource kind, required " + KIND + ", provided" + resource.getKind());
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
