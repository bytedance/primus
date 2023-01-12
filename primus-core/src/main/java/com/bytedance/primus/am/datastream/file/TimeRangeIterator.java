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

import com.bytedance.primus.proto.PrimusCommon.Time;
import com.bytedance.primus.proto.PrimusCommon.Time.TimeCase;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import com.bytedance.primus.utils.TimeUtils;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TimeRangeIterator iterates the appointed FileSourceInputs with a moving window to yield
 * FileSourceInputs with smaller TimeRanges comprised of specified dates and hours. Notes
 * <li> - TimeRanges are denoted as closed intervals.</li>
 * <li> - Now is interpreted as infinity.</li>
 */
public class TimeRangeIterator {

  public enum IterationMode {
    NONE,
    HOURLY,
    DAILY
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileScanner.class.getName());

  @Getter
  private final IterationMode mode;

  // originalInputs functions as the template used to yield new inputs and shouldn't be modified.
  private final List<FileSourceInput> originalInputs;
  private final TimeRange originalInputWindow; // [minTime, maxTime] among originalInputs

  // The states for each single batch
  @Getter
  private boolean valid;
  private Time currentBatchCursor;
  private List<FileSourceInput> generatedBatch = null;

  public TimeRangeIterator(List<FileSourceInput> inputs) throws ParseException {
    this.originalInputs = inputs;
    this.originalInputWindow = inputs.stream()
        .filter(input -> {
          // Check the existence of TimeRange
          if (!input.getTimeRange().isPresent()) {
            LOG.warn("Missing TimeRange: {}", input);
            return false;
          }
          // Check the sanity of the TimeRange
          TimeRange timeRange = input.getTimeRange().get();
          Time from = timeRange.getFrom();
          Time to = timeRange.getTo();
          if (from.getTimeCase() == TimeCase.NOW ||
              from.getTimeCase() != to.getTimeCase() && to.getTimeCase() != TimeCase.NOW
          ) {
            LOG.warn("Invalid TimeRange: {}", input);
            return false;
          }
          return true;
        })
        .map(input -> input.getTimeRange().get())
        .reduce(TimeRangeIterator::computeMinimalContainingTimeRange)
        .orElse(null);

    this.mode = originalInputWindow == null
        ? IterationMode.NONE
        : originalInputWindow.getFrom().hasDate()
            ? IterationMode.DAILY
            : originalInputWindow.getFrom().hasDateHour()
                ? IterationMode.HOURLY
                : IterationMode.NONE;

    switch (this.mode) {
      case DAILY:
        valid = true;
        currentBatchCursor = TimeUtils.newDate(
            TimeUtils.plusDay(originalInputWindow.getFrom().getDate(), -1));
        break;
      case HOURLY:
        valid = true;
        currentBatchCursor = TimeUtils.newDateHour(
            TimeUtils.plusHour(originalInputWindow.getFrom().getDateHour(), -1));
        break;
      default:
        valid = false;
        currentBatchCursor = null;
    }
  }

  public String getBatchKey() {
    if (currentBatchCursor == null) {
      return null;
    }

    switch (mode) {
      case DAILY:
        return String.valueOf(currentBatchCursor.getDate().getDate());
      case HOURLY:
        return String.format("%s%s",
            currentBatchCursor.getDateHour().getDate(),
            currentBatchCursor.getDateHour().getHour()
        );
      default:
        throw new IllegalArgumentException("Unsupported Mode: " + mode);
    }
  }

  // Returns whether there is the next batch
  public boolean prepareNextBatch() throws ParseException {
    if (!valid) {
      return false;
    }
    // There is an existing batch
    if (generatedBatch != null) {
      return true;
    }
    // Try generating new inputs
    generateNewBatch();
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
  private void generateNewBatch() throws ParseException {
    if (!valid) {
      return;
    }

    // Update batch window
    switch (mode) {
      case DAILY:
        currentBatchCursor = TimeUtils.newDate(
            TimeUtils.plusDay(currentBatchCursor.getDate(), 1));
        break;
      case HOURLY:
        currentBatchCursor = TimeUtils.newDateHour(
            TimeUtils.plusHour(currentBatchCursor.getDateHour(), 1));
        break;
      default:
        throw new IllegalArgumentException("TimeRangeIterator cannot work on " + mode);
    }

    LOG.info("CurrentBatchCursor: {}", currentBatchCursor);

    // The batch window has gone beyond the original inputs
    if (intersection(originalInputWindow, currentBatchCursor) == null) {
      LOG.info("Iterator has been exhausted");
      valid = false;
      return;
    }

    // Populate the new input batch
    generatedBatch = originalInputs.stream()
        .map(input -> {
          if (!input.getTimeRange().isPresent()) {
            return null;
          }

          TimeRange intersection = intersection(
              input.getTimeRange().get(),
              currentBatchCursor
          );
          return intersection == null ? null : new FileSourceInput(
              input.getSourceId(),
              input.getSource(),
              input.getSpec()
                  .toBuilder()
                  .setTimeRange(intersection)
                  .build());
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  // Returns a TimeRange denoted with dates or now
  private static TimeRange computeMinimalContainingTimeRange(TimeRange rangeA, TimeRange rangeB) {

    Time fromA = rangeA.getFrom();
    Time fromB = rangeB.getFrom();
    Time toA = rangeA.getTo();
    Time toB = rangeB.getTo();

    // Generate hourly TimeRange
    if (fromA.hasDateHour() && fromB.hasDateHour()) {
      return TimeRange.newBuilder()
          .setFrom(TimeUtils.min(fromA, fromB))
          .setTo((toA.hasNow() || toB.hasNow())
              ? TimeUtils.newNow() // Now is treated as infinity
              : TimeUtils.max(toA, toB))
          .build();
    }

    // Round to date and generate daily TimeRange
    return TimeRange.newBuilder()
        .setFrom(TimeUtils.min(
            TimeUtils.newDate(fromA),
            TimeUtils.newDate(fromB)))
        .setTo((toA.hasNow() || toB.hasNow())
            ? TimeUtils.newNow() // Now is treated as infinity
            : TimeUtils.max(
                TimeUtils.newDate(rangeA.getTo()),
                TimeUtils.newDate(rangeB.getTo())))
        .build();
  }

  // Compute the intersection of input and batch window
  private TimeRange intersection(TimeRange input, Time cursor) {
    // Compute cursor range
    Time inputFrom = input.getFrom();
    Time inputTo = input.getTo();
    TimeCase inputTimeCase = inputFrom.getTimeCase();
    TimeCase cursorTimeCase = cursor.getTimeCase();

    Time cursorFrom;
    Time cursorTo;
    if (inputTimeCase == cursorTimeCase) {
      cursorFrom = cursor;
      cursorTo = cursor;
    } else if (inputTimeCase == TimeCase.DATE_HOUR && cursorTimeCase == TimeCase.DATE) {
      cursorFrom = TimeUtils.newDateHour(cursor.getDate().getDate(), 0);
      cursorTo = TimeUtils.newDateHour(cursor.getDate().getDate(), 23);
    } else {
      throw new IllegalArgumentException(
          "Unexpected TimeCase pair: ("
              + "input: " + inputTimeCase
              + "cursor: " + cursorTimeCase
              + ")"
      );
    }

    // Check whether overlap
    if (TimeUtils.isBefore(cursorTo, inputFrom) ||
        !inputTo.hasNow() && TimeUtils.isAfter(cursorFrom, inputTo)
    ) {
      return null;
    }

    // Compute intersection
    return TimeUtils.newTimeRange(
        TimeUtils.max(inputFrom, cursorFrom),
        inputTo.hasNow()
            ? cursorTo
            : TimeUtils.min(inputTo, cursorTo)
    );
  }
}
