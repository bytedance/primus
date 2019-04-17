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

import com.bytedance.primus.apiserver.proto.DataProto.Time;
import com.bytedance.primus.apiserver.proto.DataProto.Time.Day;
import com.bytedance.primus.apiserver.proto.DataProto.Time.Hour;
import com.bytedance.primus.apiserver.proto.DataProto.Time.TimeFormat;
import com.bytedance.primus.apiserver.proto.DataProto.TimeRange;
import com.bytedance.primus.common.exceptions.PrimusUnsupportedException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {

  private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
      PrimusConstants.DAY_FORMAT_DEFAULT);
  private static final SimpleDateFormat hourFormat = new SimpleDateFormat(
      PrimusConstants.HOUR_FORMAT);

  /**
   * Plus day with num day
   * <p>
   * e.g. 20200630 + 1 -> 20200701
   *
   * @param day day with 8 digits
   * @param num
   * @return
   * @throws ParseException
   */
  public static String plusDay(int day, int num) throws ParseException {
    SimpleDateFormat dayFormat = new SimpleDateFormat(PrimusConstants.DAY_FORMAT_DEFAULT);
    Date dt = dayFormat.parse(String.valueOf(day));
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(dt);
    calendar.add(Calendar.DATE, num);
    return dayFormat.format(calendar.getTime());
  }

  public static Time plusDay(Time time, int num) throws ParseException {
    if (time.hasNow()) {
      throw new PrimusUnsupportedException("Time::Now cannot be computed");
    }

    Time.Day day = Time.Day.newBuilder()
        .setDay(Integer.parseInt(plusDay(time.getDate().getDay().getDay(), num)))
        .build();

    Time.Date date = time.getDate().hasHour()
        ? Time.Date.newBuilder().setDay(day).setHour(time.getDate().getHour()).build()
        : Time.Date.newBuilder().setDay(day).build();

    return Time.newBuilder()
        .setTimeFormat(time.getTimeFormat())
        .setDate(date)
        .build();
  }

  /**
   * Plus hour with num hour
   * <p>
   * eg. 2020063023 + 1 -> 2020070100, 2020070101 + 1 -> 2020070102
   *
   * @param hour hour string with 10 digits
   * @param num  the hour to plus
   * @return
   * @throws ParseException
   */
  public static String plusHour(String hour, int num) throws ParseException {
    SimpleDateFormat dayFormat = new SimpleDateFormat(
        PrimusConstants.DAY_FORMAT_DEFAULT + PrimusConstants.HOUR_FORMAT
    );
    Date dt = dayFormat.parse(hour);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(dt);
    calendar.add(Calendar.HOUR, num);
    return dayFormat.format(calendar.getTime());
  }

  /**
   * Plus (day, hour) with numHours
   * <p>
   * eg. (20200630, 23) + 1 -> 2020070100, (20200701, 0) + 1 -> 2020070101)
   *
   * @param day
   * @param hour
   * @param numHours
   * @return
   * @throws ParseException
   */
  public static String plusHour(int day, int hour, int numHours) throws ParseException {
    String time = day + String.format("%2d", hour);
    return TimeUtils.plusHour(time, numHours);
  }

  /**
   * Get day number from time string
   * <p>
   * e.g. 2020070100 -> 20200701
   *
   * @param time
   * @return
   */
  public static int getDay(String time) {
    return Integer.valueOf(time.substring(0, 8));
  }

  /**
   * Get hour number from time string
   * <p>
   * e.g. 2020070100 -> 0
   *
   * @param time
   * @return
   */
  public static int getHour(String time) {
    return Integer.valueOf(time.substring(8, 10));
  }


  /**
   * Check whether Time is before (day, hour).
   *
   * @param time
   * @param day
   * @param hour
   * @return
   */
  public static boolean isTimeBefore(Time time, int day, int hour) {
    if (time.hasNow()) {
      return false;
    }
    Time.Date date = time.getDate();
    if (date.hasHour()) {
      return date.getDay().getDay() < day ||
          (date.getDay().getDay() == day && date.getHour().getHour() < hour);
    } else {
      return date.getDay().getDay() < day;
    }
  }

  public static boolean isTimeAfter(Time a, Time b) {
    if (a.hasNow() && b.hasNow()) {
      return false;
    }
    Time base = !b.hasNow() ? b : getCurrentTime(false /* isDayGranularity */);
    return isTimeAfter(a,
        base.getDate().getDay().getDay(),
        base.getDate().getHour().getHour());
  }

  public static boolean isTimeBefore(Time a, Time b) {
    if (a.hasNow() && b.hasNow()) {
      return false;
    }
    Time base = !b.hasNow() ? b : getCurrentTime(false /* isDayGranularity */);
    return isTimeBefore(a,
        base.getDate().getDay().getDay(),
        base.getDate().getHour().getHour());
  }

  /**
   * Check whether Time is after (day, hour).
   *
   * @param time
   * @param day
   * @param hour
   * @return
   */
  public static boolean isTimeAfter(Time time, Integer day, Integer hour) {
    if (time.hasNow()) {
      return true;
    }
    Time.Date date = time.getDate();
    if (date.hasHour()) {
      return date.getDay().getDay() > day ||
          (date.getDay().getDay() == day && date.getHour().getHour() > hour);
    } else {
      return date.getDay().getDay() > day;
    }
  }

  public static Time newTime(int day) {
    return Time.newBuilder()
        .setDate(Time.Date.newBuilder()
            .setDay(Day.newBuilder().setDay(day)))
        .build();
  }

  public static Time newTime(int day, int hour) {
    return Time.newBuilder()
        .setDate(Time.Date.newBuilder()
            .setDay(Day.newBuilder().setDay(day))
            .setHour(Hour.newBuilder().setHour(hour)))
        .build();
  }

  public static Time newTime(TimeFormat timeFormat, int day) {
    return newTime(day).toBuilder()
        .setTimeFormat(timeFormat)
        .build();
  }

  public static Time newTime(TimeFormat timeFormat, int day, int hour) {
    return newTime(day, hour).toBuilder()
        .setTimeFormat(timeFormat)
        .build();
  }

  // TODO: create DateHour and move TimeFormat to input layer
  // Note: There is no TimeFormat
  public static Time getCurrentTime(boolean isDayGranularity) {
    return getCurrentTime(isDayGranularity, new Date());
  }

  public static Time getCurrentTime(boolean isDayGranularity, Date current) {
    int day = Integer.parseInt(dateFormat.format(current));
    return isDayGranularity
        ? newTime(day)
        : newTime(day, Integer.parseInt(hourFormat.format(current)));
  }

  public static Time maxTime(Time a, Time b) {
    return isTimeBefore(a, b) ? b : a;
  }

  public static Time minTime(Time a, Time b) {
    return isTimeBefore(a, b) ? a : b;
  }

  public static boolean overlapped(TimeRange a, TimeRange b) {
    if (a == null || b == null) {
      return false;
    }
    return !isTimeBefore(a.getTo(), b.getFrom()) && !isTimeBefore(b.getTo(), a.getFrom());
  }
}
