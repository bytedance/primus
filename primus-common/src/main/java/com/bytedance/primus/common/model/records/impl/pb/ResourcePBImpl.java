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


import com.bytedance.primus.common.model.records.Resource;
import com.bytedance.primus.common.model.records.ResourceInformation;
import com.bytedance.primus.common.model.util.UnitsConversionUtil;
import com.bytedance.primus.common.proto.ModelProtos.ResourceInformationProto;
import com.bytedance.primus.common.proto.ModelProtos.ResourceProto;
import java.util.Arrays;


/**
 * instantiating resource is very frequent in scheduler. hence a difference approach is taken for
 * this particular implementation. that is, we lazily create proto/builder objects to avoid object
 * allocation for non-exchanging usage (e.g. not sending/receiving through IPC). this makes
 * constructing ResourcePBImpl fairly cheap. In addition, for exchanging usage, proto instance is
 * cached and reused unless value changes.
 */
public class ResourcePBImpl extends Resource {

  ResourceProto proto = ResourceProto.getDefaultInstance();
  ResourceProto.Builder builder = null;
  boolean viaProto = false;

  // call via ProtoUtils.convertToProtoFormat(Resource)
  static ResourceProto getProto(Resource r) {
    final ResourcePBImpl pb =
        (r instanceof ResourcePBImpl)
            ? (ResourcePBImpl) r
            : new ResourcePBImpl(r.getMemoryIndex(), r.getVcoresIndex(), r.getResources());
    return pb.getProto();
  }

  public ResourcePBImpl(
      int memoryIndex, int vcoresIndex,
      ResourceInformation[] infos
  ) {
    this.memoryIndex = memoryIndex;
    this.vcoresIndex = vcoresIndex;
    this.resources = Arrays.stream(infos).map(info ->
        ResourceInformation.newInstance(
            info.getName(),
            info.getUnits(),
            info.getValue(),
            info.getResourceType(),
            info.getMinimumAllocation(),
            info.getMaximumAllocation()
        )).toArray(ResourceInformation[]::new);
  }

  public ResourcePBImpl(ResourceProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getMemory() {
    // memory should always be present
    ResourceInformation ri = resources[memoryIndex];

    if (ri.getUnits().isEmpty()) {
      return ri.getValue();
    }
    return UnitsConversionUtil.convert(ri.getUnits(),
        ResourceInformation.MEMORY_MB.getUnits(), ri.getValue());
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setMemory(long memory) {
    maybeInitBuilder();
    resources[memoryIndex].setValue(memory);
  }

  @Override
  public int getVirtualCores() {
    // vcores should always be present
    return castToIntSafely(resources[vcoresIndex].getValue());
  }

  @Override
  public void setVirtualCores(int vCores) {
    maybeInitBuilder();
    resources[vcoresIndex].setValue(vCores);
  }

  synchronized private void mergeLocalToBuilder() {
    builder
        .setMemory(this.getMemory())
        .setVirtualCores(this.getVirtualCores());

    builder.clearResourceValueMap();
    if (resources != null && resources.length != 0) {
      for (ResourceInformation resInfo : resources) {
        ResourceInformationProto.Builder e =
            ResourceInformationProto.newBuilder()
                .setKey(resInfo.getName())
                .setUnits(resInfo.getUnits())
                .setType(ProtoUtils.convertToProtoFormat(resInfo.getResourceType()))
                .setValue(resInfo.getValue());
        if (resInfo.getRanges() != null) {
          e.setValueRanges(ProtoUtils.convertToProtoFormat(resInfo.getRanges()));
        }
        builder.addResourceValueMap(e);
      }
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
}
