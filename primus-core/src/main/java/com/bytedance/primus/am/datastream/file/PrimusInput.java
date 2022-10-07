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

package com.bytedance.primus.am.datastream.file;

import com.bytedance.primus.am.datastream.file.operator.Input;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputType;

public class PrimusInput implements Input {

  private String sourceId;
  private String source;
  private String key;
  private String path;
  private InputType inputType;
  private long start;
  private long length;


  public PrimusInput(String sourceId, String source, String key, String path, InputType inputType) {
    this(sourceId, source, key, path, inputType, 0, 0);
  }

  public PrimusInput(String sourceId, String source, String key, String path, InputType inputType,
      long start, long length) {
    this.sourceId = sourceId;
    this.source = source;
    this.key = key;
    this.path = path;
    this.inputType = inputType;
    this.start = start;
    this.length = length;
  }

  public String getSourceId() {
    return sourceId;
  }

  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  @Override
  public String getSource() {
    return source;
  }

  @Override
  public void setSource(String source) {
    this.source = source;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public void setKey(String key) {
    this.key = key;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public InputType getInputType() {
    return inputType;
  }

  public void setInputType(InputType inputType) {
    this.inputType = inputType;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  @Override
  public String toString() {
    return "PrimusInput[sourceId: " + sourceId
        + ", source: " + source
        + ", key: " + key
        + ", path: " + path
        + ", inputType: " + inputType
        + "]";
  }


}
