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

package com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.basic.common.impl;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.network.NetworkConfig;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.basic.common.YarnNetworkManager;

public class YarnNetworkManagerFactory {

  public static YarnNetworkManager createNetworkManager(
      AMContext amContext,
      ApplicationAttemptId appAttemptId
  ) {
    NetworkConfig networkConfig = new NetworkConfig(amContext.getApplicationMeta().getPrimusConf());
    switch (networkConfig.getNetworkType()) {
      case OVERLAY:
        return new OverlayYarnNetworkManager(appAttemptId);
      case DEFAULT:
      default:
        return new DefaultYarnNetworkManager();
    }
  }
}
