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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IntegerUtils.class);

  public static int ensureBounded(int value, int lower, int upper) {
    return Math.min(
        upper,
        Math.max(
            lower,
            value
        )
    );
  }

  public static int ensurePositiveOrDefault(int v, int d) {
    return v > 0 ? v : d;
  }

  public static long ensurePositiveOrDefault(long v, long d) {
    return v > 0 ? v : d;
  }

  public static long maxLong(long value, long... values) {
    return Arrays.stream(values).reduce(value, Math::max);
  }

  public static void updateAtomicLongIfLarger(
      AtomicLong current,
      long desiredValue,
      String msg
  ) {
    while (true) {
      long currentValue = current.get();
      if (desiredValue <= currentValue) {
        LOG.warn(
            "Failed to update atomic long, current: {}, desired: {} msg: {}",
            currentValue, desiredValue, msg
        );
        break;
      }
      if (current.compareAndSet(currentValue, desiredValue)) {
        break;
      }
    }
  }
}
