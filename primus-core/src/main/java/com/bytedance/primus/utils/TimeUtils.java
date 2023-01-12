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
import com.bytedance.primus.proto.PrimusCommon.Time.Now;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

// TODO: Change anchor type from Time to DateHour
public class TimeUtils {

  private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("yyyyMMdd");
  private static final SimpleDateFormat defaultHourFormat = new SimpleDateFormat("HH");
  private static final SimpleDateFormat defaultDateHourFormat = new SimpleDateFormat("yyyyMMddHH");

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

  public static Date plusDay(Date date, int days) throws ParseException {
    return newDate(plusDay(date.getDate(), days)).getDate();
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

  public static DateHour plusHour(DateHour dateHour, int hours) throws ParseException {
    long numericDateHour = plusHour(dateHour.getDate() * 100L + dateHour.getHour(), hours);
    return DateHour.newBuilder()
        .setDate((int) (numericDateHour / 100))
        .setHour((int) (numericDateHour % 100))
        .build();
  }

  public static Time newNow() {
    return Time.newBuilder()
        .setNow(Now.getDefaultInstance())
        .build();
  }

  public static Time newDate(int date) {
    return Time.newBuilder()
        .setDate(Date.newBuilder()
            .setDate(date)
            .build())
        .build();
  }

  public static Time newDate(Date date) {
    return Time.newBuilder()
        .setDate(date)
        .build();
  }

  public static Time newDate(Time time) {
    switch (time.getTimeCase()) {
      case DATE:
        return time;
      case DATE_HOUR:
        return newDate(time.getDateHour().getDate());
      case NOW:
      default:
        throw new PrimusUnsupportedException("Unsupported TimeCase: " + time.getTimeCase());
    }
  }

  public static Time newDateHour(int date, int hour) {
    return Time.newBuilder()
        .setDateHour(DateHour.newBuilder()
            .setDate(date)
            .setHour(hour)
            .build())
        .build();
  }

  public static Time newDateHour(DateHour dateHour) {
    return Time.newBuilder()
        .setDateHour(dateHour)
        .build();
  }

  public static Time newDateHour(java.util.Date current) {
    return newDateHour(
        Integer.parseInt(defaultDateFormat.format(current)),
        Integer.parseInt(defaultHourFormat.format(current)));
  }

  public static TimeRange newTimeRange(Time from, Time to) {
    return TimeRange.newBuilder()
        .setFrom(from)
        .setTo(to)
        .build();
  }

  // Returns timeA is before timeB,
  public static boolean isBefore(Time timeA, Time timeB) {
    if (timeA.getTimeCase() != timeB.getTimeCase()) {
      throw new IllegalArgumentException("Cannot compare with different TimeCases"
          + timeA.getTimeCase().name()
          + timeB.getTimeCase().name());
    }

    switch (timeA.getTimeCase()) {
      case DATE:
        return timeA.getDate().getDate() < timeB.getDate().getDate();
      case DATE_HOUR:
        int dateA = timeA.getDateHour().getDate();
        int dateB = timeB.getDateHour().getDate();
        int hourA = timeA.getDateHour().getHour();
        int hourB = timeB.getDateHour().getHour();
        return dateA < dateB || dateA == dateB && hourA < hourB;
      case NOW:
      default:
        throw new IllegalArgumentException("Illegal TimeCase: " + timeA.getTimeCase().name());
    }
  }

  public static boolean isBefore(Date dateA, Date dateB) {
    return isBefore(newDate(dateA), newDate(dateB));
  }

  public static boolean isBefore(DateHour dateHourA, DateHour dateHourB) {
    return isBefore(newDateHour(dateHourA), newDateHour(dateHourB));
  }

  // Returns timeA is after timeB,
  public static boolean isAfter(Time timeA, Time timeB) {
    return isBefore(timeB, timeA);
  }

  public static boolean isAfter(Date dateA, Date dateB) {
    return isBefore(dateB, dateA);
  }

  public static boolean isAfter(DateHour dateHourA, DateHour dateHourB) {
    return isBefore(dateHourB, dateHourA);
  }

  public static Time max(Time timeA, Time timeB) {
    return isBefore(timeA, timeB) ? timeB : timeA;
  }

  public static Time min(Time timeA, Time timeB) {
    return isBefore(timeA, timeB) ? timeA : timeB;
  }
}
