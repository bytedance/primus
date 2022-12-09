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

package com.bytedance.primus.utils;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.bytedance.primus.common.exceptions.PrimusUnsupportedException;
import com.bytedance.primus.proto.PrimusCommon.Time;
import com.bytedance.primus.proto.PrimusCommon.Time.Now;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import org.junit.Test;

// TODO: Tests for DateHour
public class TestTimeUtils {

  @Test
  public void testPlusDay() throws ParseException {
    assertEquals(TimeUtils.plusDay(20201010, 5), 20201015);
    assertEquals(TimeUtils.plusDay(20201010, 25), 20201104);
    assertEquals(TimeUtils.plusDay(20201010, -10), 20200930);
  }

  @Test
  public void testPlusHour() throws ParseException {
    assertEquals(TimeUtils.plusHour(2020101000, 14), 2020101014);
    assertEquals(TimeUtils.plusHour(2020101000, 24), 2020101100);
    assertEquals(TimeUtils.plusHour(2020101000, 34), 2020101110);
    assertEquals(TimeUtils.plusHour(2020101010, -11), 2020100923);
  }

  @Test(expected = PrimusUnsupportedException.class)
  public void testPlusDayWithTime() throws ParseException {
    // Time with Now
    Time base = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();

    when(TimeUtils.plusDay(base, 0))
        .thenThrow(new PrimusUnsupportedException("Time::Now cannot be computed"));

    // Time with Day
    base = TimeUtils.newDate(20200501);

    assertEquals(TimeUtils.plusDay(base, 0), base);
    assertEquals(
        TimeUtils.plusDay(base, 1),
        TimeUtils.newDate(20200502));
    assertEquals(
        TimeUtils.plusDay(base, -1),
        TimeUtils.newDate(20200430));

    // Time with Day and Hour
    base = TimeUtils.newDateHour(20200501, 12);

    assertEquals(TimeUtils.plusDay(base, 0), base);
    assertEquals(
        TimeUtils.plusDay(base, 1),
        TimeUtils.newDateHour(20200502, 12));
    assertEquals(
        TimeUtils.plusDay(base, -1),
        TimeUtils.newDateHour(20200430, 12));
  }

  @Test
  public void testIsTimeBefore() {
    Time anchor = TimeUtils.newDateHour(new java.util.Date());

    Time time1 = TimeUtils.newDateHour(20200501, 0);
    assertEquals(TimeUtils.isTimeBefore(anchor, time1, TimeUtils.newDateHour(20200501, 0)), false);
    assertEquals(TimeUtils.isTimeBefore(anchor, time1, TimeUtils.newDateHour(20200501, 1)), true);
    assertEquals(TimeUtils.isTimeBefore(anchor, time1, TimeUtils.newDateHour(20200410, 0)), false);

    Time time2 = Time.newBuilder()
        .setDate(
            Time.Date.newBuilder()
                .setDate(20200501))
        .build();
    assertEquals(TimeUtils.isTimeBefore(anchor, time2, TimeUtils.newDateHour(20200501, 0)), false);
    assertEquals(TimeUtils.isTimeBefore(anchor, time2, TimeUtils.newDateHour(20200501, 1)), false);
    assertEquals(TimeUtils.isTimeBefore(anchor, time1, TimeUtils.newDateHour(20200502, 1)), true);
    assertEquals(TimeUtils.isTimeBefore(anchor, time2, TimeUtils.newDateHour(20200410, 0)), false);

    Time time3 = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();
    assertEquals(TimeUtils.isTimeBefore(anchor, time3, TimeUtils.newDateHour(20200501, 0)), false);
  }

  @Test
  public void testIsTimeBeforeWithTime() {
    Time anchor = TimeUtils.newDateHour(new java.util.Date());

    Time lower = TimeUtils.newDate(20000101);
    Time upper = TimeUtils.newDate(30000101);
    Time now = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();

    // With Now
    assertFalse(TimeUtils.isTimeBefore(anchor, now, now));
    assertFalse(TimeUtils.isTimeBefore(anchor, upper, now));
    assertTrue(TimeUtils.isTimeBefore(anchor, lower, now));

    // With two normalized times
    assertFalse(TimeUtils.isTimeBefore(anchor, lower, lower));
    assertFalse(TimeUtils.isTimeBefore(anchor, upper, lower));
    assertTrue(TimeUtils.isTimeBefore(anchor, lower, upper));
  }

  @Test
  public void testIsTimeAfter() {
    Time anchor = TimeUtils.newDateHour(new java.util.Date());

    Time time1 = TimeUtils.newDateHour(20200501, 0);
    assertEquals(TimeUtils.isTimeAfter(anchor, time1, TimeUtils.newDateHour(20200501, 0)), false);
    assertEquals(TimeUtils.isTimeAfter(anchor, time1, TimeUtils.newDateHour(20200430, 23)), true);
    assertEquals(TimeUtils.isTimeAfter(anchor, time1, TimeUtils.newDateHour(20200410, 0)), true);

    Time time2 = Time.newBuilder()
        .setDate(Time.Date.newBuilder()
            .setDate(20200501))
        .build();
    assertEquals(TimeUtils.isTimeAfter(anchor, time2, TimeUtils.newDateHour(20200501, 0)), false);
    assertEquals(TimeUtils.isTimeAfter(anchor, time2, TimeUtils.newDateHour(20200420, 10)), true);

    Time time3 = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();
    assertEquals(TimeUtils.isTimeAfter(anchor, time3, TimeUtils.newDateHour(20200501, 0)), true);
    assertEquals(TimeUtils.isTimeAfter(anchor, time3, TimeUtils.newDateHour(20200420, 10)), true);
  }

  @Test
  public void testIsTimeAfterWithTime() {
    Time anchor = TimeUtils.newDateHour(new java.util.Date());

    Time lower = TimeUtils.newDate(20000101);
    Time upper = TimeUtils.newDate(30000101);
    Time now = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();

    // With Now
    assertFalse(TimeUtils.isTimeAfter(anchor, now, now));
    assertTrue(TimeUtils.isTimeAfter(anchor, upper, now));
    assertFalse(TimeUtils.isTimeAfter(anchor, lower, now));

    // With two normalized times
    assertFalse(TimeUtils.isTimeAfter(anchor, lower, lower));
    assertTrue(TimeUtils.isTimeAfter(anchor, upper, lower));
    assertFalse(TimeUtils.isTimeAfter(anchor, lower, upper));
  }

  @Test
  public void testGetDateHour() {
    java.util.Date anchor = new Date(2020 - 1900, Calendar.MAY, 1, 12, 34);
    assertEquals(
        TimeUtils.newDateHour(anchor),
        TimeUtils.newDateHour(20200501, 12));
  }

  @Test
  public void testMaxTime() {
    Time anchor = TimeUtils.newDateHour(new java.util.Date());

    Time lower = TimeUtils.newDate(20000101);
    Time upper = TimeUtils.newDate(30000101);
    Time now = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();

    assertEquals(TimeUtils.maxTime(anchor, now, now), now);
    assertEquals(TimeUtils.maxTime(anchor, lower, now), now);
    assertEquals(TimeUtils.maxTime(anchor, lower, upper), upper);
    assertEquals(TimeUtils.maxTime(anchor, upper, upper), upper);
  }

  @Test
  public void testMinTime() {
    Time anchor = TimeUtils.newDateHour(new java.util.Date());

    Time lower = TimeUtils.newDate(20000101);
    Time upper = TimeUtils.newDate(30000101);
    Time now = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();

    assertEquals(TimeUtils.minTime(anchor, now, now), now);
    assertEquals(TimeUtils.minTime(anchor, lower, now), lower);
    assertEquals(TimeUtils.minTime(anchor, lower, upper), lower);
    assertEquals(TimeUtils.minTime(anchor, lower, lower), lower);
  }

  private TimeRange newTimeRange(Time s, Time e) {
    return TimeRange.newBuilder()
        .setFrom(s)
        .setTo(e)
        .build();
  }

  @Test
  public void testOverlapped() {
    Time anchor = TimeUtils.newDateHour(new java.util.Date());

    Time a = TimeUtils.newDate(20200101);
    Time b = TimeUtils.newDate(20200102);
    Time c = TimeUtils.newDate(20200103);
    Time d = TimeUtils.newDate(20200104);
    Time now = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();

    assertFalse(TimeUtils.overlapped(anchor, null, null));
    assertFalse(TimeUtils.overlapped(anchor, null, newTimeRange(b, d)));
    assertFalse(TimeUtils.overlapped(anchor, newTimeRange(a, c), null));

    assertFalse(TimeUtils.overlapped( // Disjoint
        anchor,
        newTimeRange(a, b),
        newTimeRange(c, d)
    ));

    assertTrue(TimeUtils.overlapped( // just touched
        anchor,
        newTimeRange(a, b),
        newTimeRange(b, c)
    ));

    assertTrue(TimeUtils.overlapped( // interleaved
        anchor,
        newTimeRange(a, c),
        newTimeRange(b, d)
    ));

    assertTrue(TimeUtils.overlapped( // interleaved with now
        anchor,
        newTimeRange(a, c),
        newTimeRange(b, now)
    ));

    assertTrue(TimeUtils.overlapped( // Contains
        anchor,
        newTimeRange(a, d),
        newTimeRange(b, c)
    ));

    assertTrue(TimeUtils.overlapped( // Contains with now
        anchor,
        newTimeRange(a, now),
        newTimeRange(b, c)
    ));
  }
}
