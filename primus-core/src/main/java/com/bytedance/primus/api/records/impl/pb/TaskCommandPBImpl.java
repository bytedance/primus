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

import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.TaskCommandType;
import com.bytedance.primus.proto.Primus.HeartbeatResponseProto.TaskCommandProto;
import com.bytedance.primus.proto.Primus.HeartbeatResponseProto.TaskCommandProtoOrBuilder;
import com.bytedance.primus.proto.Primus.TaskProto;

public class TaskCommandPBImpl implements TaskCommand {

  TaskCommandProto proto = TaskCommandProto.getDefaultInstance();
  TaskCommandProto.Builder builder = null;
  boolean viaProto = false;

  public TaskCommandPBImpl() {
    builder = TaskCommandProto.newBuilder();
  }

  public TaskCommandPBImpl(TaskCommandProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public TaskCommandProto getProto() {
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
      builder = TaskCommandProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public TaskCommandType getTaskCommandType() {
    TaskCommandProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getTaskCommandType());
  }

  @Override
  public void setTaskCommandType(TaskCommandType taskCommandType) {
    maybeInitBuilder();
    builder.setTaskCommandType(ProtoUtils.convertToProtoFormat(taskCommandType));
  }

  @Override
  public Task getTask() {
    TaskCommandProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasTask()) ? convertFromProtoFormat(p.getTask()) : null;
  }

  @Override
  public void setTask(Task task) {
    maybeInitBuilder();
    builder.setTask(convertToProtoFormat(task));
  }

  private TaskPBImpl convertFromProtoFormat(TaskProto p) {
    return new TaskPBImpl(p);
  }

  private TaskProto convertToProtoFormat(Task t) {
    return ((TaskPBImpl) t).getProto();
  }
}
