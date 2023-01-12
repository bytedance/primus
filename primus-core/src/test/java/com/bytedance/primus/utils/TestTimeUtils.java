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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bytedance.primus.proto.PrimusCommon.Time;
import com.bytedance.primus.proto.PrimusCommon.Time.Date;
import com.bytedance.primus.proto.PrimusCommon.Time.DateHour;
import java.text.ParseException;
import java.util.Calendar;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTimeUtils {

  private static final Time now = TimeUtils.newNow();
  private static final Time dtA = TimeUtils.newDate(20191231);
  private static final Time dtB = TimeUtils.newDate(20200101);
  private static final Time dtC = TimeUtils.newDate(20200102);
  private static final Time dhA = TimeUtils.newDateHour(20191231, 23);
  private static final Time dhB = TimeUtils.newDateHour(20200101, 0);
  private static final Time dhC = TimeUtils.newDateHour(20200101, 1);

  private void assertBooleanEquals(boolean result, boolean expectation) {
    Assertions.assertEquals(result, expectation);
  }

  @Test
  public void testPlusDay() throws ParseException {
    assertEquals(TimeUtils.plusDay(20201010, 5), 20201015);
    assertEquals(TimeUtils.plusDay(20201010, 25), 20201104);
    assertEquals(TimeUtils.plusDay(20201010, -10), 20200930);
  }

  @Test
  public void testPlusDayWithDate() throws ParseException {
    Date base = TimeUtils.newDate(20200501).getDate();
    assertEquals(
        TimeUtils.plusDay(base, 0),
        base);
    assertEquals(
        TimeUtils.plusDay(base, 1),
        TimeUtils.newDate(20200502).getDate());
    assertEquals(
        TimeUtils.plusDay(base, -1),
        TimeUtils.newDate(20200430).getDate());
  }

  @Test
  public void testPlusHour() throws ParseException {
    assertEquals(TimeUtils.plusHour(2020101000, 14), 2020101014);
    assertEquals(TimeUtils.plusHour(2020101000, 24), 2020101100);
    assertEquals(TimeUtils.plusHour(2020101000, 34), 2020101110);
    assertEquals(TimeUtils.plusHour(2020101010, -11), 2020100923);
  }

  @Test
  public void testPlusHourWithDateHour() throws ParseException {
    DateHour base = TimeUtils.newDateHour(20200101, 0).getDateHour();

    assertEquals(
        TimeUtils.plusHour(base, 0),
        base);
    assertEquals(
        TimeUtils.plusHour(base, 1),
        TimeUtils.newDateHour(20200101, 1).getDateHour());
    assertEquals(
        TimeUtils.plusHour(base, -1),
        TimeUtils.newDateHour(20191231, 23).getDateHour());
  }

  @Test
  public void testIsBefore() {
    // Test with TimeCase::Now
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isBefore(now, now));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isBefore(now, dtB));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isBefore(now, dhB));

    // Test with TimeCase::Date
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isBefore(dtB, now));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isBefore(dtB, dhB));

    assertBooleanEquals(TimeUtils.isBefore(dtB, dtA), false);
    assertBooleanEquals(TimeUtils.isBefore(dtB, dtB), false);
    assertBooleanEquals(TimeUtils.isBefore(dtB, dtC), true);

    // Test with TimeCase::DateHour
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isBefore(dhB, now));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isBefore(dhB, dtB));

    assertBooleanEquals(TimeUtils.isBefore(dhB, dhA), false);
    assertBooleanEquals(TimeUtils.isBefore(dhB, dhB), false);
    assertBooleanEquals(TimeUtils.isBefore(dhB, dhC), true);
  }

  @Test
  public void testIsAfter() {
    // Test with TimeCase::Now
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isAfter(now, now));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isAfter(now, dtB));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isAfter(now, dhB));

    // Test with TimeCase::Date
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isAfter(dtB, now));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isAfter(dtB, dhB));

    assertBooleanEquals(TimeUtils.isAfter(dtB, dtA), true);
    assertBooleanEquals(TimeUtils.isAfter(dtB, dtB), false);
    assertBooleanEquals(TimeUtils.isAfter(dtB, dtC), false);

    // Test with TimeCase::DateHour
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isAfter(dhB, now));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TimeUtils.isAfter(dhB, dtB));

    assertBooleanEquals(TimeUtils.isAfter(dhB, dhA), true);
    assertBooleanEquals(TimeUtils.isAfter(dhB, dhB), false);
    assertBooleanEquals(TimeUtils.isAfter(dhB, dhC), false);
  }

  @Test
  public void testNewDateHour() {
    java.util.Date anchor = new java.util.Date(2020 - 1900, Calendar.MAY, 1, 12, 34);
    assertEquals(
        TimeUtils.newDateHour(anchor),
        TimeUtils.newDateHour(20200501, 12));
  }
}
