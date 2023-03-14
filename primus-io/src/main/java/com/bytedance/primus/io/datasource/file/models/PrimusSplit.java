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
import lombok.Getter;

@Getter
public class PrimusSplit implements BaseSplit {

  private final int sourceId;
  private String source;
  private FileSourceSpec spec;

  private final String batchKey;
  private final String path; // path to the data file
  private long start;
  private long length;

  public PrimusSplit(PrimusInput input, String path, long start, long length) {
    this.sourceId = input.getSourceId();
    this.source = input.getSource();
    this.spec = input.getSpec();

    this.batchKey = input.getBatchKey();
    this.path = path;
    this.start = start;
    this.length = length;
  }

  // NOTE: This constructor should be only used when comparing with FileTasks
  private PrimusSplit(String batchKey, int sourceId, String path) {
    this.batchKey = batchKey;
    this.sourceId = sourceId;
    this.path = path;
  }

  @Override
  public boolean hasBeenBuilt(String batchKey, int sourceId, String path) {
    return this.compareTo(new PrimusSplit(batchKey, sourceId, path)) <= 0;
  }

  @Override
  public String toString() {
    return "PrimusSplit[sourceId: " + sourceId
        + ", source: " + source
        + ", batch key: " + batchKey
        + ", path: " + path
        + ", spec: " + spec
        + ", start: " + start
        + ", length: " + length + "]";
  }
}
