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

package com.bytedance.primus.common.event;

public enum TimelineEventType {
  PRIMUS_APP_STARTED,
  PRIMUS_CONF_EVENT,
  PRIMUS_ATTEMPT_STARTED,
  PRIMUS_VERSION,
  PRIMUS_APPLICATION_MASTER_EVENT,
  PRIMUS_APP_SHUTDOWN,
  PRIMUS_APP_FINISHED,
  PRIMUS_APP_SAVE_HISTORY,
  PRIMUS_APP_STOP_COMPONENT,
  PRIMUS_APP_STOP_NM,
  PRIMUS_TASKS_TOTAL_COUNT,

  PRIMUS_TASK_INFO_DETAILED,
  PRIMUS_TASK_INFO_FILE_SIZE,
  PRIMUS_TASK_STATE_SUCCESS_EVENT,
  PRIMUS_TASK_STATE_FAILED_EVENT,
  PRIMUS_TASK_STATE_FAILED_ATTEMPT_EVENT,

  PRIMUS_TASK_PERFORMANCE_EVENT
}
