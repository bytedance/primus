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

import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputType;
import com.bytedance.primus.apiserver.proto.DataProto.Time;
import com.bytedance.primus.apiserver.proto.DataProto.Time.TimeFormat;
import com.bytedance.primus.apiserver.proto.DataProto.TimeRange;
import com.bytedance.primus.common.collections.Pair;
import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FileSourceInput {

  @Getter
  private String sourceId;
  @Getter
  private String source;
  @Getter
  private String input;
  @Getter
  private InputType inputType;
  @Getter
  private String fileNameFilter;
  // With a customized getter
  private TimeRange timeRange;

  public static FileSourceInput newInstance(
      String sourceId,
      String source,
      String input,
      InputType inputType
  ) {
    return new FileSourceInput(sourceId, source, input, inputType, "*", null);
  }

  public static FileSourceInput newInstance(
      String sourceId,
      String source,
      String input,
      InputType inputType,
      String fileNameFilter
  ) {
    return new FileSourceInput(sourceId, source, input, inputType, fileNameFilter, null);
  }

  public static FileSourceInput newInstanceWithTimeRange(
      String sourceId,
      String source,
      String input,
      InputType inputType,
      String fileNameFilter,
      TimeRange timeRange
  ) {
    return new FileSourceInput(sourceId, source, input, inputType, fileNameFilter, timeRange);
  }

  public Optional<TimeRange> getTimeRange() {
    return Optional.ofNullable(timeRange);
  }

  public void setTimeRange(TimeRange timeRange) {
    this.timeRange = timeRange;
  }

  // TODO: Create DateHour class to abstract the parsing and error handling.
  public Pair<Integer, Integer> getStartDateHour() {
    Time time = timeRange.getFrom();
    return time.hasNow()
        ? new Pair<>(null, null)
        : new Pair<>(
            time.getDate().getDay().getDay(),
            time.getDate().getHour().getHour() // Defaults to 0
        );
  }

  // TODO: Create DateHour class to abstract the parsing and error handling.
  public Pair<Integer, Integer> getEndDateHour() {
    Time time = timeRange.getTo();
    return time.hasNow()
        ? new Pair<>(null, null)
        : new Pair<>(
            time.getDate().getDay().getDay(),
            time.getDate().getHour().getHour() // Defaults to 0
        );
  }

  public TimeFormat getTimeFormat() {
    return timeRange.getFrom().getTimeFormat();
  }

  @Override
  public String toString() {
    return "sourceId: " + sourceId +
        ", source: " + source +
        ", input: " + input +
        ", inputType: " + inputType +
        ", timeRange: " + timeRange +
        ", filter: " + fileNameFilter;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FileSourceInput)) {
      return false;
    }

    FileSourceInput other = (FileSourceInput) obj;
    return Objects.equals(sourceId, other.sourceId)
        && Objects.equals(source, other.source)
        && Objects.equals(input, other.input)
        && Objects.equals(inputType, other.inputType)
        && Objects.equals(timeRange, other.timeRange)
        && Objects.equals(fileNameFilter, other.fileNameFilter);
  }
}
