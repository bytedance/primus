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

import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.proto.Primus.TaskStatusProto;
import com.bytedance.primus.proto.Primus.TaskStatusProtoOrBuilder;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskStatusPBImpl implements TaskStatus {

  private static final Logger log = LoggerFactory.getLogger(TaskStatusPBImpl.class);
  TaskStatusProto proto = TaskStatusProto.getDefaultInstance();
  TaskStatusProto.Builder builder = null;
  boolean viaProto = false;

  private String group = null;
  private Long taskId = null;
  private String sourceId = null;
  private TaskState taskState = null;
  private Float progress = null;
  private String checkpoint = null;
  private Integer numAttempt = null;
  private Long lastAssignTime = null;
  private Long finishTime = null;
  private Long dataConsumptionTime = null;
  private String assignedNode = null;
  private String assignedNodeUrl = null;
  private String workerName = null;

  public TaskStatusPBImpl() {
    builder = TaskStatusProto.newBuilder();
  }

  public TaskStatusPBImpl(TaskStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public TaskStatusProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.group != null) {
      builder.setGroup(this.group);
    }
    if (this.taskId != null) {
      builder.setTaskId(this.taskId);
    }
    if (this.sourceId != null) {
      builder.setSourceId(sourceId);
    }
    if (this.taskState != null) {
      builder.setTaskState(ProtoUtils.convertToProtoFormat(this.taskState));
    }
    if (this.progress != null) {
      builder.setProgress(progress);
    }
    if (this.checkpoint != null) {
      builder.setCheckpoint(checkpoint);
    }
    if (this.numAttempt != null) {
      builder.setNumAttempt(numAttempt);
    }
    if (this.lastAssignTime != null) {
      builder.setLastAssignTime(lastAssignTime);
    }
    if (this.finishTime != null) {
      builder.setFinishTime(finishTime);
    }
    if (this.dataConsumptionTime != null) {
      builder.setDataConsumptionTime(dataConsumptionTime);
    }
    if (this.assignedNode != null) {
      builder.setAssignedNode(assignedNode);
    }
    if (this.assignedNodeUrl != null) {
      builder.setAssignedNodeUrl(assignedNodeUrl);
    }
    if (this.workerName != null) {
      builder.setWorkerName(workerName);
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
      builder = TaskStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getGroup() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.group != null) {
      return this.group;
    }
    return p.getGroup();
  }

  @Override
  public void setGroup(String group) {
    maybeInitBuilder();
    this.group = group;
  }

  @Override
  public long getTaskId() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    return p.getTaskId();
  }

  @Override
  public void setTaskId(long taskId) {
    maybeInitBuilder();
    this.taskId = taskId;
  }

  @Override
  public String getSourceId() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.sourceId != null) {
      return this.sourceId;
    }
    return p.getSourceId();
  }

  @Override
  public void setSourceId(String sourceId) {
    maybeInitBuilder();
    this.sourceId = sourceId;
  }

  @Override
  public TaskState getTaskState() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskState != null) {
      return this.taskState;
    }
    return ProtoUtils.convertFromProtoFormat(p.getTaskState());
  }

  @Override
  public void setTaskState(TaskState state) {
    maybeInitBuilder();
    this.taskState = state;
  }

  @Override
  public float getProgress() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.progress != null) {
      return this.progress;
    }
    return p.getProgress();
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    this.progress = progress;
  }

  @Override
  public String getCheckpoint() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.checkpoint != null) {
      return this.checkpoint;
    }
    return p.getCheckpoint();
  }

  @Override
  public void setCheckpoint(String checkpoint) {
    maybeInitBuilder();
    this.checkpoint = checkpoint;
  }

  @Override
  public int getNumAttempt() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.numAttempt != null) {
      return this.numAttempt;
    }
    return p.getNumAttempt();
  }

  @Override
  public void setNumAttempt(int numAttempt) {
    maybeInitBuilder();
    this.numAttempt = numAttempt;
  }

  @Override
  public long getLastAssignTime() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.lastAssignTime != null) {
      return this.lastAssignTime;
    }
    return p.getLastAssignTime();
  }

  @Override
  public void setLastAssignTime(long lastAssignTime) {
    maybeInitBuilder();
    this.lastAssignTime = lastAssignTime;
  }

  @Override
  public long getFinishTime() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.finishTime != null) {
      return this.finishTime;
    }
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    this.finishTime = finishTime;
  }

  @Override
  public long getDataConsumptionTime() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.dataConsumptionTime != null) {
      return this.dataConsumptionTime;
    }
    return p.getDataConsumptionTime();
  }

  @Override
  public void setDataConsumptionTime(long dataConsumptionTime) {
    maybeInitBuilder();
    this.dataConsumptionTime = dataConsumptionTime;
  }

  @Override
  public String getAssignedNode() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (assignedNode != null) {
      return assignedNode;
    }
    return p.getAssignedNode();
  }

  @Override
  public void setAssignedNode(String assignedNode) {
    maybeInitBuilder();
    this.assignedNode = assignedNode;
  }

  @Override
  public String getAssignedNodeUrl() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (assignedNodeUrl != null) {
      return assignedNodeUrl;
    }
    return p.getAssignedNodeUrl();
  }

  @Override
  public void setAssignedNodeUrl(String assignedNodeUrl) {
    maybeInitBuilder();
    this.assignedNodeUrl = assignedNodeUrl;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TaskStatusProto taskStatusProto = getProto();
    out.writeInt(taskStatusProto.getSerializedSize());
    out.write(taskStatusProto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int serializedSize = in.readInt();
    byte[] bytes = new byte[serializedSize];
    in.readFully(bytes);
    proto = TaskStatusProto.parseFrom(bytes);
    viaProto = true;
  }

  @Override
  public String toString() {
    return "TaskStatus." +
        "group[" + getGroup() + "]." +
        "taskId[" + getTaskId() + "]." +
        "sourceId[" + getSourceId() + "]." +
        "checkpoint[" + getCheckpoint() + "]." +
        "progress[" + getProgress() + "]." +
        "taskState[" + getTaskState() + "]." +
        "numAttempt[" + getNumAttempt() + "]." +
        "lastAssignTime[" + getLastAssignTime() + "].";
  }

  @Override
  public byte[] toByteArray() {
    return getProto().toByteArray();
  }

  @Override
  public String getWorkerName() {
    TaskStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (workerName != null) {
      return workerName;
    }
    return p.getWorkerName();
  }

  @Override
  public void setWorkerName(String workerName) {
    maybeInitBuilder();
    this.workerName = workerName;
  }
}
