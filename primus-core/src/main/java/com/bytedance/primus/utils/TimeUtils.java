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

import com.bytedance.primus.common.exceptions.PrimusUnsupportedException;
import com.bytedance.primus.proto.PrimusCommon.Time;
import com.bytedance.primus.proto.PrimusCommon.Time.Date;
import com.bytedance.primus.proto.PrimusCommon.Time.DateHour;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

// TODO: Change anchor type from Time to DateHour
public class TimeUtils {

  private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat(
      PrimusConstants.DATE_FORMAT_DEFAULT);
  private static final SimpleDateFormat defaultHourFormat = new SimpleDateFormat(
      PrimusConstants.HOUR_FORMAT);
  private static final SimpleDateFormat defaultDateHourFormat = new SimpleDateFormat(
      PrimusConstants.DATE_FORMAT_DEFAULT + PrimusConstants.HOUR_FORMAT
  );

  /**
   * Plus numericalDate with days, e.g. 20200630 + 1 -> 20200701
   *
   * @param numericalDate numeric date in 8 digit format
   * @param days          the days
   * @return numericalDate
   */
  public static int plusDay(int numericalDate, int days) throws ParseException {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(defaultDateFormat.parse(String.valueOf(numericalDate)));
    calendar.add(Calendar.DATE, days);
    return Integer.parseInt(defaultDateFormat.format(calendar.getTime()));
  }

  public static Time plusDay(Time time, int days) throws ParseException {
    switch (time.getTimeCase()) {
      case DATE:
        Date date = time.getDate();
        return Time
            .newBuilder()
            .setDate(Date
                .newBuilder()
                .setDate(plusDay(date.getDate(), days)))
            .build();
      case DATE_HOUR:
        DateHour dateHour = time.getDateHour();
        return Time
            .newBuilder()
            .setDateHour(DateHour
                .newBuilder()
                .setDate(plusDay(dateHour.getDate(), days))
                .setHour(dateHour.getHour()))
            .build();
      case NOW:
      default:
        throw new PrimusUnsupportedException("Unsupported TimeCase: " + time.getTimeCase());
    }
  }

  /**
   * Plus numericalDateHour by hours, e.g. 2020063023 + 1 -> 2020070100
   *
   * @param numericalDateHour hour string with 10 digits
   * @param hours             the hour to plus
   * @return numericalDateHour
   */
  public static long plusHour(long numericalDateHour, int hours) throws ParseException {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(defaultDateHourFormat.parse(String.valueOf(numericalDateHour)));
    calendar.add(Calendar.HOUR, hours);
    return Integer.parseInt(defaultDateHourFormat.format(calendar.getTime()));
  }

  public static Time plusHour(Time time, int hours) throws ParseException {
    switch (time.getTimeCase()) {
      case DATE_HOUR:
        DateHour dateHour = time.getDateHour();
        long numericDateHour = plusHour(dateHour.getDate() * 100 + dateHour.getHour(), hours);
        return Time
            .newBuilder()
            .setDateHour(DateHour
                .newBuilder()
                .setDate((int) (numericDateHour / 100))
                .setHour((int) (numericDateHour % 100)))
            .build();
      case DATE:
      case NOW:
      default:
        throw new PrimusUnsupportedException("Unsupported TimeCase: " + time.getTimeCase());
    }
  }

  public static boolean isTimeBefore(Time anchor, Time a, Time b) {
    Time concludedA = a.hasNow() ? anchor : a;
    switch (b.getTimeCase()) {
      case DATE:
        return isTimeBefore(
            concludedA,
            b.getDate().getDate(),
            0);
      case DATE_HOUR:
        return isTimeBefore(
            concludedA,
            b.getDateHour().getDate(),
            b.getDateHour().getHour());
      case NOW:
      default:
        return isTimeBefore(null /* crash intentionally */, concludedA, anchor);
    }
  }

  private static boolean isTimeBefore(Time time, int date, int hour) {
    switch (time.getTimeCase()) {
      case DATE:
        Date d = time.getDate();
        return d.getDate() < date;
      case DATE_HOUR:
        DateHour dh = time.getDateHour();
        return dh.getDate() < date || dh.getDate() == date && dh.getHour() < hour;
      case NOW:
      default:
        throw new PrimusUnsupportedException("Unsupported TimeCase: " + time.getTimeCase());
    }
  }

  public static boolean isTimeAfter(Time anchor, Time a, Time b) {
    Time concludedA = a.hasNow() ? anchor : a;
    switch (b.getTimeCase()) {
      case DATE:
        return isTimeAfter(
            concludedA,
            b.getDate().getDate(),
            23);
      case DATE_HOUR:
        return isTimeAfter(
            concludedA,
            b.getDateHour().getDate(),
            b.getDateHour().getHour());
      case NOW:
      default:
        return isTimeAfter(null /* crash intentionally */, concludedA, anchor);
    }
  }

  private static boolean isTimeAfter(Time time, int date, int hour) {
    switch (time.getTimeCase()) {
      case DATE:
        Date d = time.getDate();
        return d.getDate() > date;
      case DATE_HOUR:
        DateHour dh = time.getDateHour();
        return dh.getDate() > date || dh.getDate() == date && dh.getHour() > hour;
      case NOW:
      default:
        throw new PrimusUnsupportedException("Unsupported TimeCase: " + time.getTimeCase());
    }
  }

  // TODO: Change return type to Date
  public static Time newDate(int date) {
    return Time.newBuilder()
        .setDate(Date.newBuilder()
            .setDate(date))
        .build();
  }

  // TODO: Change return type to DateHour
  public static Time newDateHour(int date, int hour) {
    return Time.newBuilder()
        .setDateHour(DateHour.newBuilder()
            .setDate(date)
            .setHour(hour))
        .build();
  }

  // TODO: Change return type to DateHour
  public static Time newDateHour(java.util.Date current) {
    return newDateHour(
        Integer.parseInt(defaultDateFormat.format(current)),
        Integer.parseInt(defaultHourFormat.format(current)));
  }

  public static Time maxTime(Time anchor, Time a, Time b) {
    return isTimeBefore(anchor, a, b) ? b : a;
  }

  public static Time minTime(Time anchor, Time a, Time b) {
    return isTimeBefore(anchor, a, b) ? a : b;
  }

  public static boolean overlapped(Time anchor, TimeRange a, TimeRange b) {
    if (a == null || b == null) {
      return false;
    }

    boolean isBefore = isTimeBefore(anchor, a.getTo(), b.getFrom());
    boolean isAfter = isTimeAfter(anchor, a.getFrom(), b.getTo());
    return !isBefore && !isAfter;
  }
}
