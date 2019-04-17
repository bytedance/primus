/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */

package com.bytedance.primus.common.util;

import java.time.Duration;
import java.util.concurrent.Callable;

public class Sleeper {

  /**
   * sleeps for the given duration
   *
   * @param duration
   */
  // TODO: Unit test
  public static void sleep(Duration duration) throws InterruptedException {
    Thread.sleep(duration.toMillis());
  }

  /**
   * loop periodically until it returns true on the best effort basis.
   *
   * @param duration  the interval between two consecutive predicate executions
   * @param predicate the task to execute, return true to break the loop
   */
  // TODO: Unit test
  public static void loop(
      Duration duration,
      Callable<Boolean> predicate
  ) throws Exception {
    while (!predicate.call()) {
      sleep(duration);
    }
  }

  /**
   * sleepWithoutInterruptedException sleeps for the given duration and hinders InterruptedException
   * if there is any.
   *
   * @param duration
   */
  // TODO: Unit test
  public static void sleepWithoutInterruptedException(Duration duration) {
    try {
      sleep(duration);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  /**
   * loopWithoutInterruptedException periodically executes predicate until it returns true on the
   * best effort basis.
   *
   * @param duration  the interval between two consecutive predicate executions
   * @param predicate the task to execute, return true to break the loop
   */
  // TODO: Unit test
  public static void loopWithoutInterruptedException(
      Duration duration,
      Callable<Boolean> predicate
  ) throws Exception {
    while (!predicate.call()) {
      sleepWithoutInterruptedException(duration);
    }
  }
}
