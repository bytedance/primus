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

import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.model.records.NodeId;
import com.bytedance.primus.common.model.records.Priority;
import com.bytedance.primus.common.model.records.Resource;
import com.bytedance.primus.common.model.records.Token;
import com.bytedance.primus.common.proto.ModelProtos.ContainerIdProto;
import com.bytedance.primus.common.proto.ModelProtos.ContainerProto;
import com.bytedance.primus.common.proto.ModelProtos.ContainerProtoOrBuilder;
import com.bytedance.primus.common.proto.ModelProtos.NodeIdProto;
import com.bytedance.primus.common.proto.ModelProtos.PriorityProto;
import com.bytedance.primus.common.proto.ModelProtos.ResourceProto;
import com.bytedance.primus.common.proto.SecurityProtos;
import com.bytedance.primus.common.proto.SecurityProtos.TokenProto;
import java.util.Map;


public class ContainerPBImpl extends Container {

  ContainerProto proto = ContainerProto.getDefaultInstance();
  ContainerProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId = null;
  private NodeId nodeId = null;
  private Resource resource = null;
  private Priority priority = null;
  private Token containerToken = null;
  private Token infsecToken = null;
  private Map<String, String> envByRm = null;

  public ContainerPBImpl() {
    builder = ContainerProto.newBuilder();
  }

  public ContainerProto getProto() {

    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) containerId).getProto().equals(
        builder.getId())) {
      builder.setId(convertToProtoFormat(this.containerId));
    }
    if (this.nodeId != null
        && !((NodeIdPBImpl) nodeId).getProto().equals(
        builder.getNodeId())) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.priority != null &&
        !((PriorityPBImpl) this.priority).getProto().equals(
            builder.getPriority())) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.containerToken != null
        && !((TokenPBImpl) this.containerToken).getProto().equals(
        builder.getContainerToken())) {
      builder.setContainerToken(convertToProtoFormat(this.containerToken));
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

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ContainerId getId() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getId());
    return this.containerId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) {
      builder.clearNodeId();
    }
    this.nodeId = nodeId;
  }

  @Override
  public NodeId getNodeId() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setId(ContainerId id) {
    maybeInitBuilder();
    if (id == null) {
      builder.clearId();
    }
    this.containerId = id;
  }

  @Override
  public String getNodeHttpAddress() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeHttpAddress()) {
      return null;
    }
    return (p.getNodeHttpAddress());
  }

  @Override
  public void setNodeHttpAddress(String nodeHttpAddress) {
    maybeInitBuilder();
    if (nodeHttpAddress == null) {
      builder.clearNodeHttpAddress();
      return;
    }
    builder.setNodeHttpAddress(nodeHttpAddress);
  }

  @Override
  public Resource getResource() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) {
      builder.clearResource();
    }
    this.resource = resource;
  }

  @Override
  public Priority getPriority() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) {
      builder.clearPriority();
    }
    this.priority = priority;
  }

  @Override
  public Token getContainerToken() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerToken != null) {
      return this.containerToken;
    }
    if (!p.hasContainerToken()) {
      return null;
    }
    this.containerToken = convertFromProtoFormat(p.getContainerToken());
    return this.containerToken;
  }

  @Override
  public void setContainerToken(Token containerToken) {
    maybeInitBuilder();
    if (containerToken == null) {
      builder.clearContainerToken();
    }
    this.containerToken = containerToken;
  }

  @Override
  public boolean getIsGuaranteed() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIsGuaranteed();
  }

  @Override
  public void setIsGuaranteed(boolean isGuaranteed) {
    maybeInitBuilder();
    builder.setIsGuaranteed(isGuaranteed);
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl) t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority p) {
    return ((PriorityPBImpl) p).getProto();
  }

  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private SecurityProtos.TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Container: [");
    sb.append("ContainerId: ").append(getId()).append(", ");
    sb.append("NodeId: ").append(getNodeId()).append(", ");
    sb.append("NodeHttpAddress: ").append(getNodeHttpAddress()).append(", ");
    sb.append("Resource: ").append(getResource()).append(", ");
    sb.append("Priority: ").append(getPriority()).append(", ");
    sb.append("Token: ").append(getContainerToken()).append(", ");
    sb.append("]");
    return sb.toString();
  }

  //TODO Comparator
  @Override
  public int compareTo(Container other) {
    if (this.getId().compareTo(other.getId()) == 0) {
      if (this.getNodeId().compareTo(other.getNodeId()) == 0) {
        return this.getResource().compareTo(other.getResource());
      } else {
        return this.getNodeId().compareTo(other.getNodeId());
      }
    } else {
      return this.getId().compareTo(other.getId());
    }
  }
}  
