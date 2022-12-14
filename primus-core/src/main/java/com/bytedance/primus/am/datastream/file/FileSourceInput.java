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

import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.proto.PrimusCommon.Time;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class FileSourceInput {

  @Getter
  private int sourceId;
  @Getter
  private String source;
  @Getter
  private FileSourceSpec spec;

  public Optional<TimeRange> getTimeRange() {
    return spec.hasTimeRange()
        ? Optional.of(spec.getTimeRange())
        : Optional.empty();
  }

  public Pair<Integer, Integer> getStartDateHour() {
    Optional<TimeRange> optional = getTimeRange();
    if (!optional.isPresent()) {
      throw new IllegalArgumentException("Missing TimeRange: " + this);
    }

    Time time = optional.get().getFrom();
    switch (time.getTimeCase()) {
      case DATE:
        return new Pair<>(
            time.getDate().getDate(),
            0 // Defaults to 0 for closed interval
        );
      case DATE_HOUR:
        return new Pair<>(
            time.getDateHour().getDate(),
            time.getDateHour().getHour()
        );
      default:
        return new Pair<>(null, null);
    }
  }

  public Pair<Integer, Integer> getEndDateHour() {
    Optional<TimeRange> optional = getTimeRange();
    if (!optional.isPresent()) {
      throw new IllegalArgumentException("Missing TimeRange: " + this);
    }

    Time time = optional.get().getTo();
    switch (time.getTimeCase()) {
      case DATE:
        return new Pair<>(
            time.getDate().getDate(),
            23 // Defaults to 23 for closed interval
        );
      case DATE_HOUR:
        return new Pair<>(
            time.getDateHour().getDate(),
            time.getDateHour().getHour()
        );
      default:
        return new Pair<>(null, null);
    }
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
