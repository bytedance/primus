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

package com.bytedance.primus.am;

import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;

/**
 * ApplicationMaster is the center of a Primus application, which is responsible for tracking the
 * application status, orchestrating computational resources, and scheduling data inputs.
 */
public interface ApplicationMaster extends EventHandler<ApplicationMasterEvent> {

  /**
   * init() enforces every ApplicationMaster implementation relies only on PrimusConf, and thus
   * shares highly similar internal architecture.
   */
  void init(PrimusConf conf) throws Exception;

  /**
   * start() activates the ApplicationMaster which then start driving the entire Primus
   * Application.
   */
  void start() throws Exception;

  /**
   * waitForStop() is a utility that helps caller harness the exit code after the completion of the
   * Primus application
   */
  int waitForStop() throws Exception;
}
