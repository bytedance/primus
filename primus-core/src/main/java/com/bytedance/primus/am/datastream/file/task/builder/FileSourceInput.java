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

package com.bytedance.primus.am.datastream.file.task.builder;

import com.bytedance.primus.apiserver.proto.DataProto.DataSourceSpec;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class FileSourceInput {

  private int sourceId;
  private String source;
  private FileSourceSpec spec;

  public FileSourceInput(DataSourceSpec dataSourceSpec) {
    if (!dataSourceSpec.hasFileSourceSpec()) {
      throw new IllegalArgumentException(
          "Invalid dataSourceSpec of type: " + dataSourceSpec.getDataSourceCase());
    }

    this.sourceId = dataSourceSpec.getSourceId();
    this.source = dataSourceSpec.getSource();
    this.spec = dataSourceSpec.getFileSourceSpec();
  }

  public Optional<TimeRange> getTimeRange() {
    return spec.hasTimeRange()
        ? Optional.of(spec.getTimeRange())
        : Optional.empty();
  }

  @Override
  public String toString() {
    return "sourceId: " + sourceId +
        ", source: " + source +
        ", spec: " + spec;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FileSourceInput)) {
      return false;
    }

    FileSourceInput other = (FileSourceInput) obj;
    return Objects.equals(sourceId, other.sourceId)
        && Objects.equals(source, other.source)
        && spec.equals(other.getSpec());
  }
}
