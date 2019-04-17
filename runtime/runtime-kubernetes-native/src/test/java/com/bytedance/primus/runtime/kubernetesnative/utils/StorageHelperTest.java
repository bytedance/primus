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

package com.bytedance.primus.runtime.kubernetesnative.utils;


import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class StorageHelperTest {

  @Test
  public void testNewJoinedPath() {

    Path expected = new Path("/primus/staging/primus-number/__primus_conf__/log4j.properties");

    Assert.assertEquals("Base", expected,
        StorageHelper.newJoinedPath(
            "/primus/staging/primus-number/",
            "__primus_conf__/",
            "log4j.properties"
        ));

    Assert.assertEquals("Adding missing slashes", expected,
        StorageHelper.newJoinedPath(
            "/primus/staging/primus-number",
            "__primus_conf__",
            "log4j.properties"
        ));

    // RFC2396 5.2 (5): Child path is absolute
    Assert.assertEquals("Deduplicated slashes", new Path("/log4j.properties"),
        StorageHelper.newJoinedPath(
            "/primus/staging/primus-number/",
            "/__primus_conf__/",
            "/log4j.properties"
        ));
  }
}
