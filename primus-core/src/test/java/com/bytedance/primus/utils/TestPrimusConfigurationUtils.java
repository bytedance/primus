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

import com.bytedance.primus.apiserver.utils.ResourceUtils;
import com.bytedance.primus.common.util.PrimusConfigurationUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPrimusConfigurationUtils {

  @Test
  public void testLoadPrimusConf() {
    String root = "../examples";
    Arrays
        .stream(Objects.requireNonNull(new File(root).list()))
        .map(file -> String.join("/", root, file, "primus_config.json"))
        .forEach(path -> {
          try {
            ResourceUtils.buildJob(PrimusConfigurationUtils.load(path));
          } catch (IOException e) {
            Assertions.assertNull(e, "Caught an exception when processing: " + path);
          }
        });
  }
}
