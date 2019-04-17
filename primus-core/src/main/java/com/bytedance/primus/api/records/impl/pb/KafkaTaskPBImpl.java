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

import com.bytedance.primus.api.records.KafkaMessageType;
import com.bytedance.primus.api.records.KafkaStartUpMode;
import com.bytedance.primus.api.records.KafkaTask;
import com.bytedance.primus.proto.Primus.TaskProto;
import com.bytedance.primus.proto.Primus.TaskProto.KafkaTaskProto;
import com.bytedance.primus.proto.Primus.TaskProto.KafkaTaskProtoOrBuilder;
import java.util.Map;

public class KafkaTaskPBImpl implements KafkaTask {

  KafkaTaskProto proto = KafkaTaskProto.getDefaultInstance();
  KafkaTaskProto.Builder builder = null;
  boolean viaProto = false;

  public KafkaTaskPBImpl() {
    builder = KafkaTaskProto.newBuilder();
  }

  public KafkaTaskPBImpl(KafkaTaskProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public KafkaTaskProto getProto() {
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
      builder = KafkaTaskProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getTopic() {
    KafkaTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTopic();
  }

  @Override
  public void setTopic(String topic) {
    maybeInitBuilder();
    builder.setTopic(topic);
  }

  @Override
  public String getConsumerGroup() {
    KafkaTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getConsumerGroup();
  }

  @Override
  public void setConsumerGroup(String consumerGroup) {
    maybeInitBuilder();
    builder.setConsumerGroup(consumerGroup);
  }

  @Override
  public Map<String, String> getConfig() {
    KafkaTaskProtoOrBuilder p = viaProto ? proto : builder;
    return p.getConfigMap();
  }

  @Override
  public void setConfig(Map<String, String> config) {
    maybeInitBuilder();
    builder.clearConfig();
    builder.putAllConfig(config);
  }

  @Override
  public KafkaStartUpMode getKafkaStartUpMode() {
    KafkaTaskProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getKafkaStartUpMode());
  }

  @Override
  public void setKafkaStartUpMode(KafkaStartUpMode kafkaStartUpMode) {
    maybeInitBuilder();
    builder.setKafkaStartUpMode(ProtoUtils.convertToProtoFormat(kafkaStartUpMode));
  }

  @Override
  public KafkaMessageType getKafkaMessageType() {
    KafkaTaskProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getKafkaMessageType());
  }

  @Override
  public void setKafkaMessageType(KafkaMessageType kafkaMessageType) {
    maybeInitBuilder();
    builder.setKafkaMessageType(convertToProtoFormat(kafkaMessageType));
  }

  public static TaskProto.KafkaTaskProto.KafkaMessageType convertToProtoFormat(KafkaMessageType t) {
    return TaskProto.KafkaTaskProto.KafkaMessageType.valueOf(t.name());
  }

  public static KafkaMessageType convertFromProtoFormat(
      TaskProto.KafkaTaskProto.KafkaMessageType t) {
    return KafkaMessageType.valueOf(t.name());
  }
}
