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

import com.bytedance.primus.api.records.KafkaStartUpMode;
import com.bytedance.primus.api.records.TaskCommandType;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskType;
import com.bytedance.primus.proto.Primus.HeartbeatResponseProto.TaskCommandProto;
import com.bytedance.primus.proto.PrimusTask.KafkaTaskProto;
import com.bytedance.primus.proto.PrimusTask.TaskProto;
import com.bytedance.primus.proto.PrimusTask.TaskStatusProto;

public class ProtoUtils {
  public static TaskCommandProto.TaskCommandType convertToProtoFormat(TaskCommandType t) {
    return TaskCommandProto.TaskCommandType.valueOf(t.name());
  }
  public static TaskCommandType convertFromProtoFormat(TaskCommandProto.TaskCommandType t) {
    return TaskCommandType.valueOf(t.name());
  }

  public static TaskProto.TaskCase convertToProtoFormat(TaskType t) {
    return TaskProto.TaskCase.valueOf(t.name());
  }
  public static TaskType convertFromProtoFormat(TaskProto.TaskCase t) {
    return TaskType.valueOf(t.name());
  }

  public static TaskStatusProto.TaskState convertToProtoFormat(TaskState t) {
    return TaskStatusProto.TaskState.valueOf(t.name());
  }

  public static TaskState convertFromProtoFormat(TaskStatusProto.TaskState t) {
    return TaskState.valueOf(t.name());
  }

  public static KafkaTaskProto.KafkaStartUpMode convertToProtoFormat(KafkaStartUpMode t) {
    return KafkaTaskProto.KafkaStartUpMode.valueOf(t.name());
  }

  public static KafkaStartUpMode convertFromProtoFormat(KafkaTaskProto.KafkaStartUpMode t) {
    return KafkaStartUpMode.valueOf(t.name());
  }
}
