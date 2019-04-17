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

package com.bytedance.primus.executor;

public enum ExecutorExitCode {
  UNKHOWN(70),
  CONFIG_PARSE_ERROR(71),
  SETUP_PORT_FAIL(72),
  REGISTERED_FAIL(73),
  LAUNCH_FAIL(74),
  WORKER_INTERRUPTED(75),
  HEARTBEAT_FAIL(76),
  UNREGISTER_FAIL(77),
  MKFIFO_FAIL(78),
  EXPIRED(80),
  GET_EXECUTOR_FAILED(81),
  BLACKLISTED(84),
  INVALID_INET_ADDRESS(85),
  KILLED(137);

  private int exitCode;

  ExecutorExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  public int getValue() {
    return exitCode;
  }
}
