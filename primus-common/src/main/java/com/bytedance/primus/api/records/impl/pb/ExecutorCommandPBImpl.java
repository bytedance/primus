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

import com.bytedance.primus.api.records.ExecutorCommand;
import com.bytedance.primus.proto.Primus.RegisterResponseProto.ExecutorCommandProto;
import com.bytedance.primus.proto.Primus.RegisterResponseProto.ExecutorCommandProtoOrBuilder;

import java.util.Map;

public class ExecutorCommandPBImpl implements ExecutorCommand {

  ExecutorCommandProto proto = ExecutorCommandProto.getDefaultInstance();
  ExecutorCommandProto.Builder builder = null;
  boolean viaProto = false;

  public ExecutorCommandPBImpl() {
    builder = ExecutorCommandProto.newBuilder();
  }

  public ExecutorCommandPBImpl(ExecutorCommandProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ExecutorCommandProto getProto() {
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
      builder = ExecutorCommandProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getCommand() {
    ExecutorCommandProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCommand();
  }

  @Override
  public void setCommand(String command) {
    maybeInitBuilder();
    builder.setCommand(command);
  }

  @Override
  public Map<String, String> getEnvironment() {
    ExecutorCommandProtoOrBuilder p = viaProto ? proto : builder;
    return p.getEnvironmentMap();
  }

  @Override
  public void setEnvironment(Map<String, String> environment) {
    maybeInitBuilder();
    builder.clearEnvironment();
    builder.putAllEnvironment(environment);
  }

  @Override
  public int getRestartTimes() {
    ExecutorCommandProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRestartTimes();
  }

  @Override
  public void setRestartTimes(int restartTimes) {
    maybeInitBuilder();
    builder.setRestartTimes(restartTimes);
  }
}
