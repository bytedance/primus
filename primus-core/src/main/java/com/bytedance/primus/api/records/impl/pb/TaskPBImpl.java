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
import com.bytedance.primus.api.records.KafkaTask;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskType;
import com.bytedance.primus.proto.PrimusTask;
import com.bytedance.primus.proto.PrimusTask.FileTaskProto;
import com.bytedance.primus.proto.PrimusTask.KafkaTaskProto;
import com.bytedance.primus.proto.PrimusTask.TaskProto;
import com.bytedance.primus.proto.PrimusTask.TaskProtoOrBuilder;
import com.google.protobuf.TextFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaskPBImpl implements Task, Comparable<Task> {

  TaskProto proto = TaskProto.getDefaultInstance();
  TaskProto.Builder builder = null;
  boolean viaProto = false;

  private String group;
  private Long taskId = null;
  private Integer sourceId = null;
  private String source = null;
  private String checkpoint = null;
  private Integer numAttempt = null;

  public TaskPBImpl() {
    builder = TaskProto.newBuilder();
  }

  public TaskPBImpl(TaskProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public TaskProto getProto() {
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
    if (this.source != null) {
      builder.setSource(source);
    }
    if (this.checkpoint != null) {
      builder.setCheckpoint(checkpoint);
    }
    if (this.numAttempt != null) {
      builder.setNumAttempt(numAttempt);
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
      builder = TaskProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getGroup() {
    TaskProtoOrBuilder p = viaProto ? proto : builder;
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
    TaskProtoOrBuilder p = viaProto ? proto : builder;
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
  public int getSourceId() {
    TaskProtoOrBuilder p = viaProto ? proto : builder;
    if (this.sourceId != null) {
      return this.sourceId;
    }
    return p.getSourceId();
  }

  @Override
  public void setSourceId(int sourceId) {
    maybeInitBuilder();
    this.sourceId = sourceId;
  }

  @Override
  public String getSource() {
    TaskProtoOrBuilder p = viaProto ? proto : builder;
    if (this.source != null) {
      return this.source;
    }
    return p.getSource();
  }

  @Override
  public void setSource(String source) {
    maybeInitBuilder();
    this.source = source;
  }

  @Override
  public String getCheckpoint() {
    PrimusTask.TaskProtoOrBuilder p = viaProto ? proto : builder;
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
  public TaskType getTaskType() {
    TaskProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getTaskCase());
  }

  @Override
  public FileTask getFileTask() {
    TaskProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasFileTask()) ? convertFromProtoFormat(p.getFileTask()) : null;
  }

  @Override
  public void setFileTask(FileTask fileTask) {
    maybeInitBuilder();
    builder.setFileTask(convertToProtoFormat(fileTask));
  }

  @Override
  public KafkaTask getKafkaTask() {
    TaskProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasKafkaTask()) ? convertFromProtoFormat(p.getKafkaTask()) : null;
  }

  @Override
  public void setKafkaTask(KafkaTask kafkaTask) {
    maybeInitBuilder();
    builder.setKafkaTask(convertToProtoFormat(kafkaTask));
  }

  @Override
  public int getNumAttempt() {
    TaskProtoOrBuilder p = viaProto ? proto : builder;
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
  public String getUid() {
    return getGroup() + "#" + getTaskId();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TaskProto taskProto = getProto();
    out.writeInt(taskProto.getSerializedSize());
    out.write(taskProto.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int serializedSize = in.readInt();
    byte[] bytes = new byte[serializedSize];
    in.readFully(bytes);
    proto = TaskProto.parseFrom(bytes);
    viaProto = true;
  }

  @Override
  public int compareTo(Task other) {
    assert (getGroup().equals(other.getGroup()));
    if (this.getTaskId() < other.getTaskId()) {
      return -1;
    } else if (this.getTaskId() == other.getTaskId()) {
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private FileTaskPBImpl convertFromProtoFormat(FileTaskProto p) {
    return new FileTaskPBImpl(p);
  }

  private FileTaskProto convertToProtoFormat(FileTask t) {
    return ((FileTaskPBImpl) t).getProto();
  }

  private KafkaTaskPBImpl convertFromProtoFormat(KafkaTaskProto p) {
    return new KafkaTaskPBImpl(p);
  }

  private KafkaTaskProto convertToProtoFormat(KafkaTask t) {
    return ((KafkaTaskPBImpl) t).getProto();
  }
}
