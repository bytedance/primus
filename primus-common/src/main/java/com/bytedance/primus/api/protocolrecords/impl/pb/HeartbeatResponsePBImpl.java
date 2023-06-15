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

import com.bytedance.primus.api.records.ExecutorCommandType;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.impl.pb.TaskCommandPBImpl;
import com.bytedance.primus.api.protocolrecords.HeartbeatResponse;
import com.bytedance.primus.proto.Primus.HeartbeatResponseProto;
import com.bytedance.primus.proto.Primus.HeartbeatResponseProtoOrBuilder;
import com.bytedance.primus.proto.Primus.HeartbeatResponseProto.TaskCommandProto;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HeartbeatResponsePBImpl implements HeartbeatResponse {

  HeartbeatResponseProto proto =
      HeartbeatResponseProto.getDefaultInstance();
  HeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;

  public HeartbeatResponsePBImpl() {
    builder = HeartbeatResponseProto.newBuilder();
  }

  public HeartbeatResponsePBImpl(HeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public HeartbeatResponseProto getProto() {
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
      builder = HeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<TaskCommand> getTaskCommands() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskCommand> taskCommands = new ArrayList<TaskCommand>();
    for (TaskCommandProto command : p.getTaskCommandsList()) {
      taskCommands.add(convertFromProtoFormat(command));
    }
    return taskCommands;
  }

  @Override
  public void setTaskCommands(List<TaskCommand> taskCommands) {
    maybeInitBuilder();
    builder.clearTaskCommands();
    builder.addAllTaskCommands(new Iterable<TaskCommandProto>() {
      @Override
      public Iterator<TaskCommandProto> iterator() {
        return new Iterator<TaskCommandProto>() {
          Iterator<TaskCommand> iter = taskCommands.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskCommandProto next() {
            return convertToProtoFormat(iter.next());
          }
        };
      }
    });
  }

  @Override
  public ExecutorCommandType getExecutorCommandType() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getExecutorCommandType());
  }

  @Override
  public void setExecutorCommandType(ExecutorCommandType executorCommandType) {
    maybeInitBuilder();
    builder.setExecutorCommandType(ProtoUtils.convertToProtoFormat(executorCommandType));
  }

  @Override
  public boolean isTaskReady() {
    HeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTaskReady();
  }

  @Override
  public void setTaskReady(boolean taskReady) {
    maybeInitBuilder();
    builder.setTaskReady(taskReady);
  }

  private TaskCommandPBImpl convertFromProtoFormat(TaskCommandProto p) {
    return new TaskCommandPBImpl(p);
  }

  private TaskCommandProto convertToProtoFormat(TaskCommand t) {
    return ((TaskCommandPBImpl) t).getProto();
  }
}
