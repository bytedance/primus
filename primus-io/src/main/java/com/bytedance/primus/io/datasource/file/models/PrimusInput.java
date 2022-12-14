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

package com.bytedance.primus.io.datasource.file.models;

import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PrimusInput implements BaseInput {

  private int sourceId;
  private String source;
  private FileSourceSpec spec;

  private String batchKey;
  private String path; // path to the data file or the containing directory

  public PrimusInput(
      int sourceId,
      String source,
      String batchKey,
      String path,
      FileSourceSpec spec
  ) {
    this(sourceId, source, spec, batchKey, path);
  }

  @Override
  public String toString() {
    return "PrimusInput[sourceId: " + sourceId
        + ", source: " + source
        + ", batch key: " + batchKey
        + ", path: " + path
        + ", spec: " + spec
        + "]";
  }
}
