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

package com.bytedance.primus.am.master;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.bytedance.primus.common.child.Child;
import com.bytedance.primus.common.child.ChildContext;
import com.bytedance.primus.common.child.ChildEvent;
import com.bytedance.primus.common.child.ChildExitedEvent;
import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.common.event.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master implements Child {

  private static final Logger LOG = LoggerFactory.getLogger(Master.class);

  private AMContext amContext;
  private MasterContext masterContext;
  private Dispatcher dispatcher;
  private volatile boolean success = false;

  public Master(AMContext amContext, MasterContext masterContext, Dispatcher dispatcher) {
    this.amContext = amContext;
    this.masterContext = masterContext;
    this.dispatcher = dispatcher;
  }

  @Override
  public ChildContext getContext() {
    return masterContext;
  }

  @Override
  public ChildLaunchPlugin getLaunchPlugin() {
    return null;
  }

  @Override
  public void handle(ChildEvent event) {
    switch (event.getType()) {
      case CHILD_STARTED:
        LOG.info("Master is running...");
        break;
      case LAUNCH_FAILED:
      case CHILD_INTERRUPTED:
        failApp(event.getMessage());
        break;
      case CHILD_EXITED:
        int exitCode = ((ChildExitedEvent) event).getExitCode();
        if (exitCode == 0) {
          LOG.info("Master exited with 0");
          success = true;
        } else {
          failApp(event.getMessage());
        }
        break;
    }
  }

  public boolean isSuccess() {
    return success;
  }

  private void failApp(String diagnosis) {
    LOG.error("Master is failed for " + diagnosis);
    dispatcher.getEventHandler()
        .handle(new ApplicationMasterEvent(amContext, ApplicationMasterEventType.FAIL_ATTEMPT,
            diagnosis, ApplicationExitCode.MASTER_FAILED.getValue()));
  }
}
