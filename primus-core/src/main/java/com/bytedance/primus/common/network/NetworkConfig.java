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

package com.bytedance.primus.common.network;

import com.bytedance.primus.proto.PrimusCommon.RunningMode;
import com.bytedance.primus.proto.PrimusConfOuterClass.NetworkConfig.NetworkType;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import lombok.Getter;

public class NetworkConfig {

  @Getter
  private final RunningMode runningMode;
  @Getter
  private final boolean keepIpAndPortUnderOverlay;
  @Getter
  private final NetworkType networkType;

  public NetworkConfig(PrimusConf primusConf) {
    switch (primusConf.getRunningMode()) {
      case YARN:
      case KUBERNETES:
        this.runningMode = primusConf.getRunningMode();
        this.networkType = primusConf
            .getScheduler()
            .getNetworkConfig()
            .getNetworkType();
        this.keepIpAndPortUnderOverlay = primusConf
            .getScheduler()
            .getNetworkConfig()
            .getKeepIpPortUnderOverlay();
        break;
      default:
        throw new IllegalArgumentException("Unknown RunningMode: " + primusConf.getRunningMode());
    }
  }

  public NetworkEndpointTypeEnum getNetworkEndpointType() {
    switch (runningMode) {
      case KUBERNETES:
        return NetworkEndpointTypeEnum.IPADDRESS;
      case YARN:
        return networkType == NetworkType.OVERLAY && isKeepIpAndPortUnderOverlay()
            ? NetworkEndpointTypeEnum.IPADDRESS
            : NetworkEndpointTypeEnum.HOSTNAME;
      default:
        throw new IllegalArgumentException("Unsupported RunningMode: " + runningMode);
    }
  }

  @Override
  public String toString() {
    return String.format(
        "NetworkConfig={RunningMode: %s, NetworkType: %s, KeepIpAndPortUnderOverlay: %s}",
        runningMode, networkType, keepIpAndPortUnderOverlay
    );
  }
}
