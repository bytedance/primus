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

package com.bytedance.primus.apiserver.records;

import com.bytedance.primus.apiserver.proto.DataProto;
import com.bytedance.primus.apiserver.records.impl.DataSpecImpl;
import com.google.protobuf.util.JsonFormat;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDataSpec {

  private static DataSpec dataSpec = null;

  @BeforeClass
  public static void setup() throws Exception {
    String dataSpecJsonPath = "src/test/resources/data_spec.json";
    InputStream in = new FileInputStream(dataSpecJsonPath);
    Reader reader = new InputStreamReader(in);
    JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
    DataProto.DataSpec.Builder builder = DataProto.DataSpec.newBuilder();
    parser.merge(reader, builder);
    dataSpec = new DataSpecImpl(builder.build());
  }

  @Test
  public void testGetProto() {
    dataSpec.getProto();
  }
}
