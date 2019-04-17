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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.bytedance.primus.api.records.InputType;
import com.bytedance.primus.apiserver.proto.DataProto.Time;
import com.bytedance.primus.apiserver.proto.DataProto.Time.Now;
import com.bytedance.primus.apiserver.proto.DataProto.Time.TimeFormat;
import com.bytedance.primus.apiserver.proto.DataProto.TimeRange;
import com.bytedance.primus.utils.TimeUtils;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;

// TODO: Test with hourly granularity
// TODO: Provide negative test cases
public class TestTimeRangeIterator {

  Time FOREVER = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 30000101);

  private Time newTime(int date) {
    return TimeUtils.newTime(date);
  }

  // TODO: Test with InputType as well.
  private FileSourceInput newFileSourceInput(String key, Time start, Time end) {
    return FileSourceInput.newInstanceWithTimeRange(
        key, // sourceID
        key, // source
        key, // input
        InputType.TEXT_INPUT,
        key, // fileNameFilter
        TimeRange.newBuilder()
            .setFrom(start)
            .setTo(end)
            .build()
    );
  }

  // Drops the last time is the length of the input is odd.
  private List<FileSourceInput> newFileSourceInputList(Time... times) {
    List<FileSourceInput> ret = new LinkedList<>();
    for (int i = 0; i < times.length; i += 2) {
      ret.add(newFileSourceInput("KEY", times[i], times[i + 1]));
    }

    return ret;
  }

  @Test
  public void testEmptyInput() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(new LinkedList<>());
    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertNull(iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());
    // 1st iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testConsecutivePreparationsWithoutPopping() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(iter.getNextBatchEndTime(), newTime(20191231));
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st preparation
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101)),
        iter.peekNextBatch());

    // 2nd preparation
    assertTrue(iter.prepareNextBatch(FOREVER)); //
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101)),
        iter.peekNextBatch());
  }

  @Test
  public void testSingleInputSmallerThanBatchWindow() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testSingleInputEqualToBatchWindow() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101), newTime(20200104)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testSingleInputBiggerThanBatchWindow() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101), newTime(20200106)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200105), iter.getNextBatchStartTime());
    assertEquals(newTime(20200108), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200105), newTime(20200106)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 3rd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testSingleInputWithSmallerNow() throws ParseException {
    Time now = TimeUtils.newTime(20200102);
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101),
            Time.newBuilder()
                .setNow(Now.getDefaultInstance())
                .build()));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(now));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200102), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200102)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(now));
  }

  @Test
  public void testSingleInputWithExactNow() throws ParseException {
    Time now = TimeUtils.newTime(20200104);
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101),
            Time.newBuilder()
                .setNow(Now.getDefaultInstance())
                .build()));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(now));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(now));
  }

  @Test
  public void testSingleInputWithBiggerNow() throws ParseException {
    Time now = TimeUtils.newTime(20200106);
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101),
            Time.newBuilder()
                .setNow(Now.getDefaultInstance())
                .build()));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(now));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertTrue(iter.prepareNextBatch(now));
    assertEquals(newTime(20200105), iter.getNextBatchStartTime());
    assertEquals(newTime(20200106), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200105), newTime(20200106)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 3rd iteration
    assertFalse(iter.prepareNextBatch(now));
  }

  @Test
  public void testContinuousInputs() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101),
            newTime(20200102), newTime(20200102),
            newTime(20200103), newTime(20200103),
            newTime(20200104), newTime(20200104)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101),
            newTime(20200102), newTime(20200102),
            newTime(20200103), newTime(20200103),
            newTime(20200104), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testContinuousInputsUnsorted() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200103), newTime(20200103),
            newTime(20200102), newTime(20200102),
            newTime(20200101), newTime(20200101),
            newTime(20200104), newTime(20200104)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200103), newTime(20200103),
            newTime(20200102), newTime(20200102),
            newTime(20200101), newTime(20200101),
            newTime(20200104), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testOverlappedInputs() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101), newTime(20200103),
            newTime(20200102), newTime(20200104)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200103),
            newTime(20200102), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testDisjointedInputs() throws ParseException {
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101),
            newTime(20200104), newTime(20200104)));

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        newFileSourceInputList(
            newTime(20200101), newTime(20200101),
            newTime(20200104), newTime(20200104)),
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }

  @Test
  public void testScatteredInputsWithDifferentKeys() throws ParseException {

    TimeRangeIterator iter = new TimeRangeIterator(
        new LinkedList<FileSourceInput>() {{
          add(newFileSourceInput("A", newTime(20200101), newTime(20200101)));
          add(newFileSourceInput("B", newTime(20200103), newTime(20200103)));
          add(newFileSourceInput("C", newTime(20200105), newTime(20200105)));
          add(newFileSourceInput("D", newTime(20200107), newTime(20200107)));
        }});

    // Initial values
    assertNull(iter.getNextBatchStartTime());
    assertEquals(newTime(20191231), iter.getNextBatchEndTime());
    assertEquals(new LinkedList<>(), iter.peekNextBatch());

    // 1st iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200101), iter.getNextBatchStartTime());
    assertEquals(newTime(20200104), iter.getNextBatchEndTime());
    assertEquals(
        new LinkedList<FileSourceInput>() {{
          add(newFileSourceInput("A", newTime(20200101), newTime(20200101)));
          add(newFileSourceInput("B", newTime(20200103), newTime(20200103)));
        }},
        iter.peekNextBatch());
    iter.popNextBatch();

    // 2nd iteration
    assertTrue(iter.prepareNextBatch(FOREVER));
    assertEquals(newTime(20200105), iter.getNextBatchStartTime());
    assertEquals(newTime(20200108), iter.getNextBatchEndTime());
    assertEquals(
        new LinkedList<FileSourceInput>() {{
          add(newFileSourceInput("C", newTime(20200105), newTime(20200105)));
          add(newFileSourceInput("D", newTime(20200107), newTime(20200107)));
        }},
        iter.peekNextBatch());
    iter.popNextBatch();

    // 3rd iteration
    assertFalse(iter.prepareNextBatch(FOREVER));
  }
}
