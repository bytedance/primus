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

package com.bytedance.primus.common.utils;

import com.bytedance.primus.common.util.StringUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class StringUtilsTest {

  @Test
  public void testCompareTimeStampStrings() {
    String[] timestamps = {
        "0", "1", "11", "12", "111", "123",
        "123", "111", "12", "11", "1", "0"
    };
    Assert.assertArrayEquals(
        new String[]{"0", "0", "1", "1", "11", "11", "12", "12", "111", "111", "123", "123"},
        Arrays.stream(timestamps)
            .sorted(StringUtils::compareTimeStampStrings)
            .toArray(String[]::new)
    );
    Assert.assertArrayEquals(
        new String[]{"123", "123", "111", "111", "12", "12", "11", "11", "1", "1", "0", "0"},
        Arrays.stream(timestamps)
            .sorted((a, b) -> -1 * StringUtils.compareTimeStampStrings(a, b))
            .toArray(String[]::new)
    );
  }

  @Test
  public void testFromTemplateAndDictionary() {
    String pattern = "/root/{{YYYY}}{{MM}}{{DD}}/{{HH}}/";
    Map<String, String> dict = new HashMap<String, String>() {{
      put("\\{\\{YYYY\\}\\}", "2020");
      put("\\{\\{MM\\}\\}", "01");
      put("\\{\\{DD\\}\\}", "01");
      put("\\{\\{HH\\}\\}", "00");
    }};
    Assert.assertEquals(
        "/root/20200101/00/",
        StringUtils.genFromTemplateAndDictionary(pattern, dict)
    );
  }
}
