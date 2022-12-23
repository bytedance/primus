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

package com.bytedance.primus.am.eventlog;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ExecutorCompleteEvent extends ExecutorEvent {

  private final AMContext context;
  private final SchedulerExecutor executor;

  public ExecutorCompleteEvent(AMContext context, SchedulerExecutor executor) {
    super(ExecutorEventType.COMPLETED);
    this.context = context;
    this.executor = executor;
  }

  public SchedulerExecutor getExecutor() {
    return executor;
  }

  @Override
  public String toJsonString() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("event", ExecutorCompleteEvent.class.getCanonicalName());
    json.put("timestamp", getTimestamp());
    json.put("eventType", getType());
    json.put("executorState", executor.getExecutorState());
    json.put("launchTime", executor.getLaunchTime());
    json.put("releaseTime", executor.getReleaseTime());
    json.put("logUrl", context.getMonitorInfoProvider().getExecutorLogUrl(executor));
    json.put("executorExitCode", executor.getExecutorExitCode());
    json.put("executorExitMsg", executor.getContainerExitMsg());
    json.put("roleName", executor.getExecutorId().getRoleName());
    json.put("roleUniqueId", executor.getExecutorId().toUniqString());
    return json.toString().replace("\\", "");
  }
}
