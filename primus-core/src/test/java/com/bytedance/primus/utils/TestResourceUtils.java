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

import static org.junit.Assert.assertEquals;

import com.bytedance.primus.apiserver.proto.DataProto.Time.TimeFormat;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Test;

public class TestResourceUtils {

  @Test
  public void testBuildJob() {
    String root = "../examples";
    Arrays
        .stream(Objects.requireNonNull(new File(root).list()))
        .map(file -> String.join("/", root, file, "primus_config.json"))
        .forEach(path -> {
          try {
            ResourceUtils.buildJob(FileUtils.buildPrimusConf(path));
          } catch (IOException e) {
            Assert.assertNull("Caught an exception when processing: " + path, e);
          }
        });
  }

  @Test
  public void testGetPrefix() {
    String expect = "hdfs://prefix/yet-another-prefix/";
    assertEquals(ResourceUtils.getPrefix(expect), expect);

    String case1 = "hdfs://prefix/yet-another-prefix/20200514/";
    String prefix1 = ResourceUtils.getPrefix(case1);
    assertEquals(prefix1, expect);
    assertEquals(ResourceUtils.getTimeFormat(case1, prefix1), TimeFormat.TF_DEFAULT);

    String case2 = "hdfs://prefix/yet-another-prefix/2020-05-14/";
    String prefix2 = ResourceUtils.getPrefix(case2);
    assertEquals(prefix2, expect);
    assertEquals(ResourceUtils.getTimeFormat(case2, prefix2), TimeFormat.TF_DASH);

    String case3 = "hdfs://prefix/yet-another-prefix/202005140000-202005140000/";
    String prefix3 = ResourceUtils.getPrefix(case3);
    assertEquals(prefix3, expect);
    assertEquals(ResourceUtils.getTimeFormat(case3, prefix3), TimeFormat.TF_RANGE);
  }
}
