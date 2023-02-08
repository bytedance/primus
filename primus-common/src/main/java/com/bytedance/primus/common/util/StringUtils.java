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

import com.bytedance.primus.common.collections.Pair;
import com.google.common.base.Strings;
import java.util.Iterator;
import java.util.Map;

/**
 * General string utils
 */

public class StringUtils {

  /**
   * Returns the input string if it exists and not empty. Otherwise, returns the default value.
   *
   * @param input        string to check
   * @param defaultValue string servers as the default (can be null or empty)
   * @return validates string
   */
  public static String ensure(String input, String defaultValue) {
    return Strings.isNullOrEmpty(input) ? defaultValue : input;
  }

  /**
   * Concatenates strings, using a separator.
   *
   * @param separator Separator to join with.
   * @param strings   Strings to join.
   */
  public static String join(CharSequence separator, Iterable<?> strings) {
    Iterator<?> i = strings.iterator();
    if (!i.hasNext()) {
      return "";
    }
    StringBuilder sb = new StringBuilder(i.next().toString());
    while (i.hasNext()) {
      sb.append(separator);
      sb.append(i.next().toString());
    }
    return sb.toString();
  }

  /**
   * @param timestampA
   * @param timestampB
   * @return
   */
  public static int compareTimeStampStrings(String timestampA, String timestampB) {
    int lenA = timestampA.length();
    int lenB = timestampB.length();
    return lenA == lenB
        ? timestampA.compareTo(timestampB)
        : (lenA > lenB) ? 1 : -1;
  }

  // Returns the last token of the input string delimited by the delimiter if there are tokens
  // generated from the input else empty string is returned.
  public static String getLastToken(String input, String delimiter) {
    String[] tokens = input.split(delimiter);
    return tokens.length != 0
        ? tokens[tokens.length - 1]
        : "";
  }

  // Returns a string built by replacing every occurrence of the dictionary keys in template string
  // with their corresponding dictionary values. Note that, the dictionary is not ordered.
  public static String genFromTemplateAndDictionary(String template, Map<String, String> dict) {
    if (template == null) {
      return null;
    }

    return dict.entrySet().stream()
        .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
        .reduce(new Pair<>("placeholder", template), (acc, pair) -> new Pair<>(
            acc.getKey(),
            acc.getValue().replaceAll(
                pair.getKey(),
                pair.getValue()
            )
        )).getValue();
  }
}
