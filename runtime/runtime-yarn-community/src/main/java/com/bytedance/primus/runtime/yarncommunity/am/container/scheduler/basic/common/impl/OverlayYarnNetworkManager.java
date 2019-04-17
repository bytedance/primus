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

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.basic.common.NetworkConstants;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.basic.common.YarnNetworkManager;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverlayYarnNetworkManager implements YarnNetworkManager {

  private static final Logger log = LoggerFactory.getLogger(OverlayYarnNetworkManager.class);

  private ApplicationAttemptId applicationAttemptId;

  public OverlayYarnNetworkManager(ApplicationAttemptId applicationAttemptId) {
    this.applicationAttemptId = applicationAttemptId;
  }

  public Map<String, String> createNetworkEnvMap(ExecutorId executorId) {
    Map<String, String> networkEnvMap = new HashMap<>();
    networkEnvMap.put(NetworkConstants.ENV_YARN_CONTAINER_RUNTIME_CONTAINERD_CONTAINER_NETWORK,
        NetworkConstants.ENV_YARN_CONTAINER_RUNTIME_CONTAINERD_CONTAINER_NETWORK_VALUE_OVERLAY);

    String ipAddressUniqKey = getOverlayNetworkIPAddressUniqKey(executorId);
    log.info("Executor IPAddressUniqKey: " + ipAddressUniqKey);
    networkEnvMap.put(
        NetworkConstants.ENV_YARN_CONTAINER_RUNTIME_CONTAINERD_CONTAINER_NETWORK_IPV4_UNIQUE_KEY,
        ipAddressUniqKey);

    return networkEnvMap;

  }

  private String getOverlayNetworkIPAddressUniqKey(ExecutorId executorId) {
    String appAttemptIdStr = applicationAttemptId.toString();
    String executorIdStr = executorId.toString();
    String uniqKey = String.format("%s#%s", appAttemptIdStr, executorIdStr);
    return uniqKey;
  }

}
