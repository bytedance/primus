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

import com.bytedance.primus.apiserver.proto.DataProto.Time;
import com.bytedance.primus.apiserver.proto.DataProto.Time.Day;
import com.bytedance.primus.apiserver.proto.DataProto.Time.Now;
import com.bytedance.primus.apiserver.proto.DataProto.Time.TimeFormat;
import com.bytedance.primus.apiserver.proto.DataProto.TimeRange;
import com.bytedance.primus.common.exceptions.PrimusUnsupportedException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import org.junit.Test;

public class TestTimeUtils {

  @Test
  public void testPlusDay() throws ParseException {
    assertEquals(TimeUtils.plusDay(20201010, 5), "20201015");
    assertEquals(TimeUtils.plusDay(20201010, 25), "20201104");
    assertEquals(TimeUtils.plusDay(20201010, -10), "20200930");
  }

  @Test
  public void testPlusHour() throws ParseException {
    assertEquals(TimeUtils.plusHour("2020101000", 14), "2020101014");
    assertEquals(TimeUtils.plusHour("2020101000", 24), "2020101100");
    assertEquals(TimeUtils.plusHour("2020101000", 34), "2020101110");
    assertEquals(TimeUtils.plusHour("2020101010", -11), "2020100923");

    assertEquals(TimeUtils.plusHour(20201010, 0, 14), "2020101014");
    assertEquals(TimeUtils.plusHour(20201010, 0, 24), "2020101100");
    assertEquals(TimeUtils.plusHour(20201010, 0, 34), "2020101110");
    assertEquals(TimeUtils.plusHour(20201010, 10, -11), "2020100923");
  }

  @Test
  public void testGetDay() {
    assertEquals(TimeUtils.getDay("2020101000"), 20201010);
    assertEquals(TimeUtils.getDay("2020101001"), 20201010);
  }

  @Test(expected = PrimusUnsupportedException.class)
  public void testPlusDayWithTime() throws ParseException {
    // Time with Now
    Time base = Time.newBuilder()
        .setTimeFormat(TimeFormat.TF_DEFAULT)
        .setNow(Now.getDefaultInstance())
        .build();

    when(TimeUtils.plusDay(base, 0))
        .thenThrow(new PrimusUnsupportedException("Time::Now cannot be computed"));

    // Time with Day
    base = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200501);

    assertEquals(TimeUtils.plusDay(base, 0), base);
    assertEquals(
        TimeUtils.plusDay(base, 1),
        TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200502));
    assertEquals(
        TimeUtils.plusDay(base, -1),
        TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200430));

    // Time with Day and Hour
    base = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200501, 12);

    assertEquals(TimeUtils.plusDay(base, 0), base);
    assertEquals(
        TimeUtils.plusDay(base, 1),
        TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200502, 12));
    assertEquals(
        TimeUtils.plusDay(base, -1),
        TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200430, 12));
  }

  @Test
  public void testGetHour() {
    assertEquals(TimeUtils.getHour("2020101000"), 0);
    assertEquals(TimeUtils.getHour("2020101001"), 1);
  }

  @Test
  public void testIsTimeBefore() {
    Time time1 = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200501, 0);
    assertEquals(TimeUtils.isTimeBefore(time1, 20200501, 0), false);
    assertEquals(TimeUtils.isTimeBefore(time1, 20200501, 1), true);
    assertEquals(TimeUtils.isTimeBefore(time1, 20200410, 0), false);

    Time time2 = Time.newBuilder()
        .setDate(
            Time.Date.newBuilder().setDay(
                Day.newBuilder().setDay(20200501)))
        .build();
    assertEquals(TimeUtils.isTimeBefore(time2, 20200501, 0), false);
    assertEquals(TimeUtils.isTimeBefore(time2, 20200501, 1), false);
    assertEquals(TimeUtils.isTimeBefore(time1, 20200502, 1), true);
    assertEquals(TimeUtils.isTimeBefore(time2, 20200410, 0), false);

    Time time3 = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();
    assertEquals(TimeUtils.isTimeBefore(time3, 20200501, 0), false);
  }

  @Test
  public void testIsTimeBeforeWithTime() {
    Time lower = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20000101);
    Time upper = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 30000101);
    Time now = Time.newBuilder()
        .setTimeFormat(TimeFormat.TF_DEFAULT)
        .setNow(Now.getDefaultInstance())
        .build();

    // With Now
    assertFalse(TimeUtils.isTimeBefore(now, now));
    assertFalse(TimeUtils.isTimeBefore(upper, now));
    assertTrue(TimeUtils.isTimeBefore(lower, now));

    // With two normalized times
    assertFalse(TimeUtils.isTimeBefore(lower, lower));
    assertFalse(TimeUtils.isTimeBefore(upper, lower));
    assertTrue(TimeUtils.isTimeBefore(lower, upper));
  }

  @Test
  public void testIsTimeAfter() {
    Time time1 = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200501, 0);
    assertEquals(TimeUtils.isTimeAfter(time1, 20200501, 0), false);
    assertEquals(TimeUtils.isTimeAfter(time1, 20200430, 23), true);
    assertEquals(TimeUtils.isTimeAfter(time1, 20200410, 0), true);

    Time time2 = Time.newBuilder()
        .setDate(
            Time.Date.newBuilder().setDay(
                Day.newBuilder().setDay(20200501)))
        .build();
    assertEquals(TimeUtils.isTimeAfter(time2, 20200501, 0), false);
    assertEquals(TimeUtils.isTimeAfter(time2, 20200420, 10), true);

    Time time3 = Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();
    assertEquals(TimeUtils.isTimeAfter(time3, 20200501, 0), true);
    assertEquals(TimeUtils.isTimeAfter(time3, 20200420, 10), true);
  }

  @Test
  public void testIsTimeAfterWithTime() {
    Time lower = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20000101);
    Time upper = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 30000101);
    Time now = Time.newBuilder()
        .setTimeFormat(TimeFormat.TF_DEFAULT)
        .setNow(Now.getDefaultInstance())
        .build();

    // With Now
    assertFalse(TimeUtils.isTimeAfter(now, now));
    assertTrue(TimeUtils.isTimeAfter(upper, now));
    assertFalse(TimeUtils.isTimeAfter(lower, now));

    // With two normalized times
    assertFalse(TimeUtils.isTimeAfter(lower, lower));
    assertTrue(TimeUtils.isTimeAfter(upper, lower));
    assertFalse(TimeUtils.isTimeAfter(lower, upper));
  }

  @Test
  public void testGetCurrentTime() {
    Date current = new Date(2020 - 1900, Calendar.MAY, 1, 12, 34);
    assertEquals(
        TimeUtils.getCurrentTime(true, current),
        TimeUtils.newTime(20200501));
    assertEquals(
        TimeUtils.getCurrentTime(false, current),
        TimeUtils.newTime(20200501, 12));
  }

  @Test
  public void testMaxTime() {
    Time lower = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20000101);
    Time upper = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 30000101);
    Time now = Time.newBuilder()
        .setTimeFormat(TimeFormat.TF_DEFAULT)
        .setNow(Now.getDefaultInstance())
        .build();

    assertEquals(TimeUtils.maxTime(now, now), now);
    assertEquals(TimeUtils.maxTime(lower, now), now);
    assertEquals(TimeUtils.maxTime(lower, upper), upper);
    assertEquals(TimeUtils.maxTime(upper, upper), upper);
  }

  @Test
  public void testMinTime() {
    Time lower = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20000101);
    Time upper = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 30000101);
    Time now = Time.newBuilder()
        .setTimeFormat(TimeFormat.TF_DEFAULT)
        .setNow(Now.getDefaultInstance())
        .build();

    assertEquals(TimeUtils.minTime(now, now), now);
    assertEquals(TimeUtils.minTime(lower, now), lower);
    assertEquals(TimeUtils.minTime(lower, upper), lower);
    assertEquals(TimeUtils.minTime(lower, lower), lower);
  }

  private TimeRange newTimeRange(Time s, Time e) {
    return TimeRange.newBuilder()
        .setFrom(s)
        .setTo(e)
        .build();
  }

  @Test
  public void testOverlapped() {
    Time a = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200101);
    Time b = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200102);
    Time c = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200103);
    Time d = TimeUtils.newTime(TimeFormat.TF_DEFAULT, 20200104);
    Time now = Time.newBuilder()
        .setTimeFormat(TimeFormat.TF_DEFAULT)
        .setNow(Now.getDefaultInstance())
        .build();

    assertFalse(TimeUtils.overlapped(null, null));
    assertFalse(TimeUtils.overlapped(null, newTimeRange(b, d)));
    assertFalse(TimeUtils.overlapped(newTimeRange(a, c), null));

    assertFalse(TimeUtils.overlapped( // Disjoint
        newTimeRange(a, b),
        newTimeRange(c, d)));

    assertTrue(TimeUtils.overlapped( // just touched
        newTimeRange(a, b),
        newTimeRange(b, c)));

    assertTrue(TimeUtils.overlapped( // interleaved
        newTimeRange(a, c),
        newTimeRange(b, d)));

    assertTrue(TimeUtils.overlapped( // interleaved with now
        newTimeRange(a, c),
        newTimeRange(b, now)));

    assertTrue(TimeUtils.overlapped( // Contains
        newTimeRange(a, d),
        newTimeRange(b, c)));

    assertTrue(TimeUtils.overlapped( // Contains with now
        newTimeRange(a, now),
        newTimeRange(b, c)));
  }
}
