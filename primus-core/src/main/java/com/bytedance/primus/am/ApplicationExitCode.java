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

public enum ApplicationExitCode {
  UNDEFINED(-5000),
  FAIL_ATTEMPT(-5001),
  FAIL_APP(-5002),
  RUNTIME_ERROR(-5003),

  CONTAINER_COMPLETE(-5040),
  TASK_SUCCEED(-5041),
  SUCCESS_BY_RPC(-5042),
  KILLED_BY_RPC(-5060),
  ABORT(-5062),
  FAILOVER(-5064),
  SCHEDULE_TIMEOUT(-5065),
  GDPR(-5066),
  SUSPEND(-5067),
  EXCEED_TASK_FAIL_PERCENT(-5068),
  YARN_REGISTRY_FAILED(-5069),
  CONFIG_PARSE_ERROR(-5071),
  GANG_SCHEDULE_FAILED(-5072),
  WRONG_FS(-5073),
  MASTER_FAILED(-5074),
  TIMEOUT_WITH_NO_PENDING_TASK(-5075),
  MASTER_SUCCEEDED(-5077),
  BUILD_TASK_FAILED(-5078),
  DATA_STREAM_FAILED(-5079),
  DATA_INCOMPATIBLE(-5080),

  KILLED_BY_HTTP(-5061),
  FAIL_ATTEMPT_BY_HTTP(-5081),
  SUCCESS_BY_HTTP(-5082),

  KUBERNETES_ENV_MISSING(-5100),
  EXECUTOR_OOM(-5104),
  GANG_POLICY_FAILED(-5173);
  private int exitCode;

  ApplicationExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  public int getValue() {
    return exitCode;
  }
}
