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

import com.bytedance.primus.apiserver.proto.DataProto.Time;
import com.bytedance.primus.apiserver.proto.DataProto.TimeRange;
import com.bytedance.primus.utils.TimeUtils;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Move to UTC and always down to hourly granularity
// TODO: Create DateHour class to replace using Time to avoid overly complicated exceptions.
// TimeRangeIterator iterates the appointed FileSourceInputs with a moving window to yield
// FileSourceInputs with smaller TimeRanges comprised of specified dates and hours.
// NOTE: Now is interpreted in local timezone and TimeRanges are denoted as closed intervals.
public class TimeRangeIterator {

  private static final Logger LOG = LoggerFactory.getLogger(FileScanner.class.getName());
  private static final int DAY_WINDOW_SIZE = 4; // Generates smaller intervals to allow pipelining.

  // originalInputs functions as the template used to yield new inputs and shouldn't be modified.
  private final List<FileSourceInput> originalInputs;
  private final TimeRange originalInputWindow; // [minTime, maxTime] among originalInputs

  // The states for each single batch
  private boolean valid = false;
  private Time generatedBatchStartTime = null; // Aligned to day
  private Time generatedBatchEndTime = null;   // Aligned to day
  private List<FileSourceInput> generatedBatch = null;

  public TimeRangeIterator(List<FileSourceInput> inputs) throws ParseException {
    originalInputs = inputs;
    originalInputWindow = inputs.stream()
        .filter(input -> input.getTimeRange().isPresent())
        .map(input -> input.getTimeRange().get())
        .reduce((TimeRange a, TimeRange b) ->
            TimeRange.newBuilder()
                .setFrom(TimeUtils.minTime(a.getFrom(), b.getFrom()))
                .setTo(TimeUtils.maxTime(a.getTo(), b.getTo()))
                .build())
        .orElse(null);

    if (originalInputWindow != null) {
      valid = true;
      generatedBatchStartTime = null;
      generatedBatchEndTime = TimeUtils.plusDay(
          originalInputWindow.getFrom(),
          -1 // Move to the day before the minimum date as the initial generatedInputBatchEndTime.
      );
    }
  }

  // Returns there is the next batch
  public boolean prepareNextBatch() throws ParseException {
    return prepareNextBatch(TimeUtils.getCurrentTime(true /* isDayGranularity */));
  }

  public boolean prepareNextBatch(Time current) throws ParseException {
    if (!valid) {
      return false;
    }
    // There is an existing batch
    if (generatedBatch != null) {
      return true;
    }
    // Try generating new inputs
    generateNewBatch(current);
    // Final judgement
    return generatedBatch != null;
  }

  // Returns empty list when null.
  public List<FileSourceInput> peekNextBatch() {
    return generatedBatch != null
        ? generatedBatch
        : new LinkedList<>();
  }

  public void popNextBatch() {
    generatedBatch = null;
  }

  // Try generating a new batch till the iterator becomes invalid.
  private void generateNewBatch(Time current) throws ParseException {
    while (valid && generatedBatch == null) {
      // Compute batch window
      Time batchWindowStart = TimeUtils.plusDay(generatedBatchEndTime, 1);
      Time batchWindowEnd = TimeUtils.minTime(
          TimeUtils.plusDay(generatedBatchEndTime, DAY_WINDOW_SIZE),
          TimeUtils.newTime(current.getDate().getDay().getDay()));

      LOG.info("Current BatchWindow: [{}.{}, {}.{}]",
          batchWindowStart.getDate().getDay().getDay(),
          batchWindowStart.getDate().getHour().getHour(),
          batchWindowEnd.getDate().getDay().getDay(),
          batchWindowEnd.getDate().getHour().getHour());

      // The batch window has gone beyond the original inputs
      if (TimeUtils.isTimeAfter(
          batchWindowStart,
          TimeUtils.minTime(current, originalInputWindow.getTo())
      )) {
        LOG.info("Iterator has been exhausted");
        valid = false;
        break;
      }

      // Populate the new input batch
      TimeRange batchWindow = TimeRange.newBuilder()
          .setFrom(batchWindowStart)
          .setTo(batchWindowEnd)
          .build();

      generatedBatchStartTime = batchWindowStart;
      generatedBatchEndTime = batchWindowEnd;
      generatedBatch = originalInputs.stream()
          .filter(input -> {
            Optional<TimeRange> timeRange = input.getTimeRange();
            return timeRange.isPresent() && TimeUtils.overlapped(batchWindow, timeRange.get());
          })
          .map(input -> {
            TimeRange inputWindow = input.getTimeRange().get();
            LOG.info("fileSourceInput: " + input
                + "inputStartDay: " + inputWindow.getFrom().getDate().getDay().getDay()
                + "inputStartHour: " + inputWindow.getFrom().getDate().getHour().getHour()
                + "inputEndDay: " + inputWindow.getTo().getDate().getDay().getDay()
                + "inputEndHour: " + inputWindow.getTo().getDate().getHour().getHour());

            return FileSourceInput.newInstanceWithTimeRange(
                input.getSourceId(),
                input.getSource(),
                input.getInput(),
                input.getInputType(),
                input.getFileNameFilter(),
                TimeRange.newBuilder()
                    .setFrom(TimeUtils.maxTime(
                        input.getTimeRange().get().getFrom(),
                        generatedBatchStartTime))
                    .setTo(TimeUtils.minTime(
                        input.getTimeRange().get().getTo(),
                        generatedBatchEndTime))
                    .build()
            );
          })
          .collect(Collectors.toList());
    }
  }

  public Time getNextBatchStartTime() {
    return generatedBatchStartTime;
  }

  public Time getNextBatchEndTime() {
    return generatedBatchEndTime;
  }

}
