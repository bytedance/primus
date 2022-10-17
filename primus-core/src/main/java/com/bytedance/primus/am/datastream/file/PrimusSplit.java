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
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Getter
@Setter
public class PrimusSplit implements BaseSplit {

  private String sourceId;
  private String source;

  private String path;
  private long start;
  private long length;

  private String key;
  private FileSourceSpec spec; // TODO: Reuse spec for other fields.

  @Override
  public int compareTo(Input p) {
    PrimusSplit other = (PrimusSplit) p;
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
        + ", spec: " + spec
        + ", start: " + start
        + ", length: " + length + "]";
  }
}
