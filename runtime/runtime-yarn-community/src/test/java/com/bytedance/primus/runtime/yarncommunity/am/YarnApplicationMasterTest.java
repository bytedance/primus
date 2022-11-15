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

package com.bytedance.primus.runtime.yarncommunity.am;

import static org.mockito.Mockito.doAnswer;

import org.apache.hadoop.yarn.client.api.NMClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class YarnApplicationMasterTest {

  @Mock
  private NMClient nmClient;

  @Test
  public void testStopNMClient() {
    doAnswer(t -> {
      Thread.sleep(2 * 1000);
      return null;
    }).when(nmClient).stop();
    YarnApplicationMaster yarnApplicationMaster = new YarnApplicationMaster(null);
    Assert.assertTrue(yarnApplicationMaster.stopNMClientWithTimeout(nmClient));
    YarnApplicationMaster.STOP_NM_CLIENT_TIMEOUT = 1;
    Assert.assertFalse(yarnApplicationMaster.stopNMClientWithTimeout(nmClient));
  }
}
