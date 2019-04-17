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
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusRuntime;
import com.bytedance.primus.proto.PrimusRuntime.NetworkConfig.NetworkType;

public class NetworkConfigHelper {

  public static NetworkConfig getNetworkConfig(PrimusConf primusConf) {
    NetworkConfig networkConfig = new NetworkConfig();
    switch (primusConf.getRunningMode()) {
      case YARN:
        networkConfig = extractNetworkConfig(
            primusConf.getScheduler().getYarnScheduler().getNetworkConfig());
        networkConfig.setRunningMode(RunningMode.YARN);
        break;
      case KUBERNETES:
        networkConfig = extractNetworkConfig(
            primusConf.getScheduler().getKubernetesScheduler().getNetworkConfig());
        networkConfig.setRunningMode(RunningMode.KUBERNETES);
        break;
    }
    return networkConfig;
  }

  private static NetworkConfig extractNetworkConfig(
      PrimusRuntime.NetworkConfig networkConfigProto) {
    NetworkConfig networkConfig = new NetworkConfig();
    networkConfig.setKeepIpAndPortUnderOverlay(networkConfigProto.getKeepIpPortUnderOverlay());
    if (NetworkType.OVERLAY == networkConfigProto.getNetworkType()) {
      networkConfig.setNetworkTypeEnum(NetworkTypeEnum.OVERLAY);
    } else {
      networkConfig.setNetworkTypeEnum(NetworkTypeEnum.DEFAULT);
    }
    return networkConfig;
  }

  public static NetworkEndpointTypeEnum getNetworkEndpointType(NetworkConfig config) {
    if (config.getRunningMode() == RunningMode.KUBERNETES) {
      return NetworkEndpointTypeEnum.IPADDRESS;
    }
    if(NetworkTypeEnum.OVERLAY == config.getNetworkTypeEnum() && config
        .isKeepIpAndPortUnderOverlay()){
      return NetworkEndpointTypeEnum.IPADDRESS;
    }else{
      return NetworkEndpointTypeEnum.HOSTNAME;
    }
  }
}
