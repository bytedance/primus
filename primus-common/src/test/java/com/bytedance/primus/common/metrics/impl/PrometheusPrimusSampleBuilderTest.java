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

package com.bytedance.primus.common.metrics.impl;

import com.bytedance.primus.common.metrics.impl.PrometheusPrimusSampleBuilder.MetricId;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class PrometheusPrimusSampleBuilderTest {

  @Test
  public void TestMetricId() {
    // TODO: Enrich test coverage
    MetricId metricId = new MetricId("metric_name{app_id=primus-0001,role=chief}");
    Assert.assertEquals("metric_name", metricId.name);
    Assert.assertEquals(new HashMap<String, String>() {{
      put("app_id", "primus-0001");
      put("role", "chief");
    }}, metricId.labels);
  }
}
