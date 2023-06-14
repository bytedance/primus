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
import com.bytedance.primus.api.records.ExecutorState;
import com.bytedance.primus.proto.Primus;
import com.bytedance.primus.proto.Primus.HeartbeatRequestProto;

public class ProtoUtils {
  public static HeartbeatRequestProto.ExecutorState convertToProtoFormat(ExecutorState e) {
    return HeartbeatRequestProto.ExecutorState.valueOf(e.name());
  }
  public static ExecutorState convertFromProtoFormat(HeartbeatRequestProto.ExecutorState e) {
    return ExecutorState.valueOf(e.name());
  }
  public static Primus.HeartbeatResponseProto.ExecutorCommandType convertToProtoFormat(
      ExecutorCommandType t) {
    return Primus.HeartbeatResponseProto.ExecutorCommandType.valueOf(t.name());
  }
  public static ExecutorCommandType convertFromProtoFormat(Primus.HeartbeatResponseProto.ExecutorCommandType t) {
    return ExecutorCommandType.valueOf(t.name());
  }
}
