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

/**
 * BaseSplit is the most granular unit of Primus data scheduling, which represents a file for
 * executors to read and inject to their corresponding trainer processes.
 */
public interface BaseSplit extends BaseInput, Comparable<BaseSplit> {

  String getPath(); // The path to the data file on file system.

  boolean hasBeenBuilt(String batchKey, int sourceId, String path);

  default int compareTo(BaseSplit other) {
    int compareBatchKey = getBatchKey().compareTo(other.getBatchKey());
    if (compareBatchKey != 0) {
      return compareBatchKey < 0 ? -1 : 1;
    }
    int compareSourceId = getSourceId() - other.getSourceId();
    if (compareSourceId != 0) {
      return compareSourceId < 0 ? -1 : 1;
    }
    int comparePath = getPath().compareTo(other.getPath());
    if (comparePath != 0) {
      return comparePath < 0 ? -1 : 1;
    }
    return 0;
  }
}
