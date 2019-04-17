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

public class NetworkConfig {

  private NetworkTypeEnum networkTypeEnum = NetworkTypeEnum.DEFAULT;

  private RunningMode runningMode = RunningMode.LOCAL;

  private boolean keepIpAndPortUnderOverlay = false;

  public NetworkTypeEnum getNetworkTypeEnum() {
    return networkTypeEnum;
  }

  public void setNetworkTypeEnum(NetworkTypeEnum networkTypeEnum) {
    this.networkTypeEnum = networkTypeEnum;
  }

  public boolean isKeepIpAndPortUnderOverlay() {
    return keepIpAndPortUnderOverlay;
  }

  public void setKeepIpAndPortUnderOverlay(boolean keepIpAndPortUnderOverlay) {
    this.keepIpAndPortUnderOverlay = keepIpAndPortUnderOverlay;
  }

  public RunningMode getRunningMode() {
    return runningMode;
  }

  public void setRunningMode(RunningMode runningMode) {
    this.runningMode = runningMode;
  }

  @Override
  public String toString() {
    return "NetworkConfig{" +
        "netWorkTypeEnum=" + networkTypeEnum +
        ", keepIpAndPortUnderOverlay=" + keepIpAndPortUnderOverlay +
        '}';
  }
}
