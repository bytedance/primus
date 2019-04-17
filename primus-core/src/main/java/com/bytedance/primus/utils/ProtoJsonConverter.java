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

package com.bytedance.primus.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoJsonConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoJsonConverter.class);

  private boolean includingDefaultValueFields;
  private JsonFormat.TypeRegistry.Builder typeRegistryBuilder;
  private JsonFormat.Printer printer;
  private JsonFormat.Parser parser;

  public ProtoJsonConverter() {
    this(false);
  }

  public ProtoJsonConverter(boolean includingDefaultValueFields) {
    this.includingDefaultValueFields = includingDefaultValueFields;
    typeRegistryBuilder = JsonFormat.TypeRegistry.newBuilder();
    printer = JsonFormat.printer();
    if (includingDefaultValueFields) {
      printer = printer.includingDefaultValueFields();
    }
    parser = JsonFormat.parser();
  }

  public ProtoJsonConverter(List<Descriptors.Descriptor> anyFieldDescriptors) {
    typeRegistryBuilder = JsonFormat.TypeRegistry.newBuilder();
    addDescriptors(anyFieldDescriptors);
  }

  public void addDescriptor(Descriptors.Descriptor anyFieldDescriptor) {
    addDescriptors(Arrays.asList(anyFieldDescriptor));
  }

  public void addDescriptors(List<Descriptors.Descriptor> anyFieldDescriptors) {
    typeRegistryBuilder.add(anyFieldDescriptors);
    JsonFormat.TypeRegistry typeRegistry = typeRegistryBuilder.build();
    printer = JsonFormat.printer().usingTypeRegistry(typeRegistry);
    if (includingDefaultValueFields) {
      printer = printer.includingDefaultValueFields();
    }
    parser = JsonFormat.parser().usingTypeRegistry(typeRegistry);
  }

  public String toJson(Message sourceMessage) throws IOException {
    String json = printer.print(sourceMessage);
    return json;
  }

  public Message toProto(Message.Builder targetBuilder, String json) throws IOException {
    parser.merge(json, targetBuilder);
    return targetBuilder.build();
  }

  public static String getJsonString(MessageOrBuilder messageOrBuilder) {
    try {
      return JsonFormat.printer().print(messageOrBuilder);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Failed to get json from proto", e);
      return "Failed to get json from proto";
    }
  }

  public static String getJsonStringWithDefaultValueFields(MessageOrBuilder messageOrBuilder)
      throws InvalidProtocolBufferException {
    return JsonFormat
        .printer()
        .includingDefaultValueFields()
        .print(messageOrBuilder);
  }
}
