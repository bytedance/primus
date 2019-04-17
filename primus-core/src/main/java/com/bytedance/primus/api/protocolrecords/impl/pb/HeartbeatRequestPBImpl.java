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

import com.bytedance.primus.api.protocolrecords.HeartbeatRequest;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.ExecutorState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import com.bytedance.primus.proto.Primus.ExecutorIdProto;
import com.bytedance.primus.proto.Primus.HeartbeatRequestProto;
import com.bytedance.primus.proto.Primus.HeartbeatRequestProtoOrBuilder;
import com.bytedance.primus.proto.Primus.TaskStatusProto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HeartbeatRequestPBImpl implements HeartbeatRequest {
  HeartbeatRequestProto proto =
      HeartbeatRequestProto.getDefaultInstance();
  HeartbeatRequestProto.Builder builder = null;
  boolean viaProto = false;

  public HeartbeatRequestPBImpl() {
    builder = HeartbeatRequestProto.newBuilder();
  }

  public HeartbeatRequestPBImpl(HeartbeatRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public HeartbeatRequestProto getProto() {
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
      builder = HeartbeatRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ExecutorId getExecutorId() {
    HeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasExecutorId()) ? convertFromProtoFormat(p.getExecutorId()) : null;
  }

  @Override
  public void setExecutorId(ExecutorId executorId) {
    maybeInitBuilder();
    builder.setExecutorId(convertToProtoFormat(executorId));
  }

  @Override
  public ExecutorState getExecutorState() {
    HeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getExecutorState());
  }

  @Override
  public void setExecutorState(ExecutorState executorState) {
    maybeInitBuilder();
    builder.setExecutorState(ProtoUtils.convertToProtoFormat(executorState));
  }

  @Override
  public boolean getNeedMoreTask() {
    HeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getNeedMoreTask();
  }

  @Override
  public void setNeedMoreTask(boolean needMoreTask) {
    maybeInitBuilder();
    builder.setNeedMoreTask(needMoreTask);
  }

  @Override
  public List<TaskStatus> getTaskStatuses() {
    HeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskStatus> taskStatuses = new ArrayList<TaskStatus>();
    for (TaskStatusProto status : p.getTaskStatusesList()) {
      taskStatuses.add(convertFromProtoFormat(status));
    }
    return taskStatuses;
  }

  @Override
  public void setTaskStatuses(List<TaskStatus> taskStatuses) {
    maybeInitBuilder();
    builder.clearTaskStatuses();
    builder.addAllTaskStatuses(new Iterable<TaskStatusProto>() {
      @Override
      public Iterator<TaskStatusProto> iterator() {
        return new Iterator<TaskStatusProto>() {
          Iterator<TaskStatus> iter = taskStatuses.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskStatusProto next() {
            return convertToProtoFormat(iter.next());
          }
        };
      }
    });
  }

  private ExecutorIdPBImpl convertFromProtoFormat(ExecutorIdProto p) {
    return new ExecutorIdPBImpl(p);
  }

  private ExecutorIdProto convertToProtoFormat(ExecutorId t) {
    return ((ExecutorIdPBImpl) t).getProto();
  }

  private TaskStatusPBImpl convertFromProtoFormat(TaskStatusProto p) {
    return new TaskStatusPBImpl(p);
  }

  private TaskStatusProto convertToProtoFormat(TaskStatus t) {
    return ((TaskStatusPBImpl) t).getProto();
  }
}
