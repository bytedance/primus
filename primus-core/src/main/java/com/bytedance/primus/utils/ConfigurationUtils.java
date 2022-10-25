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

import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

public abstract class ConfigurationUtils {

  public static PrimusConf loadPrimusConf(String configPath) throws IOException {
    try {
      return (PrimusConf) buildMessageFromJson(configPath, PrimusConf.newBuilder());
    } catch (IOException e) {
      throw new IOException("Config parse failed", e);
    }
  }

  static public Configuration newEnvConf(PrimusConf primusConf) {
    Configuration forged = new BaseConfiguration();
    primusConf.getYarnConfMap().forEach(forged::addProperty);
    return forged;
  }

  static public org.apache.hadoop.conf.Configuration newHadoopConf(PrimusConf primusConf) {
    org.apache.hadoop.conf.Configuration forged = new org.apache.hadoop.conf.Configuration();
    primusConf.getYarnConfMap().forEach(forged::set);
    return forged;
  }

  static private Message buildMessageFromJson(
      String jsonPath,
      Message.Builder builder
  ) throws IOException {
    try (InputStream in = Files.newInputStream(Paths.get(jsonPath))) {
      Reader reader = new InputStreamReader(in);
      JsonFormat.Parser parser = JsonFormat
          .parser()
          .ignoringUnknownFields();
      parser.merge(reader, builder);
      return builder.build();
    }
  }
}
