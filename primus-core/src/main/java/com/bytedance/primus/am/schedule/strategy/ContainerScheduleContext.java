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

package com.bytedance.primus.am.schedule.strategy;

import com.bytedance.blacklist.BlacklistTracker;
import com.bytedance.primus.common.model.records.Container;
import java.util.Optional;

public class ContainerScheduleContext {

  private String hostName;
  private Optional<BlacklistTracker> blacklistTrackerOp;
  private int roleIndex;
  private Container container;
  private int exitCode;
  private String errMsg;

  public ContainerScheduleContext(Container container, int exitCode, String errMsg,
      Optional<BlacklistTracker> blacklistTrackerOp) {
    this.exitCode = exitCode;
    this.errMsg = errMsg;
    this.roleIndex = container.getPriority().getPriority();
    this.container = container;
    this.hostName = container.getNodeId().getHost();
    this.blacklistTrackerOp = blacklistTrackerOp;
  }

  public ContainerScheduleContext(Container container) {
    this(container, -1, "", Optional.empty());
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public Container getContainer() {
    return container;
  }

  public void setContainer(Container container) {
    this.container = container;
  }

  public int getRoleIndex() {
    return roleIndex;
  }

  public void setRoleIndex(int roleIndex) {
    this.roleIndex = roleIndex;
  }

  public int getExitCode() {
    return exitCode;
  }

  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  public String getErrMsg() {
    return errMsg;
  }

  public void setErrMsg(String errMsg) {
    this.errMsg = errMsg;
  }

  public Optional<BlacklistTracker> getBlacklistTrackerOp() {
    return blacklistTrackerOp;
  }

  @Override
  public String toString() {
    return "ContainerScheduleContext{" +
        "hostName='" + hostName + '\'' +
        ", roleIndex=" + roleIndex +
        ", container=" + container.getId() +
        ", exitCode=" + exitCode +
        ", errMsg=" + errMsg +
        '}';
  }
}
