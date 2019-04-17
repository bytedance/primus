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
import com.bytedance.primus.api.records.InputType;

public class PrimusSplit extends BaseSplit {

  private String sourceId;
  private String source;
  private String key;
  private String path;
  private InputType inputType;
  private String table;
  private long start;
  private long length;

  public PrimusSplit(String sourceId, String source, String key, String path, long start,
      long length, InputType inputType, String table) {
    this.sourceId = sourceId;
    this.source = source;
    this.key = key;
    this.path = path;
    this.inputType = inputType;
    this.table = table;
    this.start = start;
    this.length = length;
  }

  public String getSourceId() {
    return sourceId;
  }

  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getKey() {
    return key;
  }

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

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
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
  public int compareTo(Input p) {
    PrimusSplit other = (PrimusSplit)p;
    int ret = getKey().compareTo(other.getKey());
    if (ret == 0) {
      ret = getSource().compareTo(other.getSource());
    }
    if (ret == 0) {
      ret = getPath().compareTo(other.getPath());
    }
    return ret;
  }

  @Override
  public String toString() {
    return "PrimusSplit[sourceId: " + getSourceId()
        + ", source: " + getSource()
        + ", key: " + getKey()
        + ", path: " + getPath()
        + ", inputType: " + getInputType()
        + ", table: " + getTable()
        + ", start: " + start
        + ", length: " + length + "]";
  }
}
