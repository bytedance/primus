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

import com.bytedance.primus.api.records.ExecutorId;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ExecutorStartEvent extends ExecutorEvent {

  private String containerId;
  private ExecutorId executorId;

  public ExecutorStartEvent(String containerId, ExecutorId executorId) {
    super(ExecutorEventType.START);
    this.containerId = containerId;
    this.executorId = executorId;
  }

  @Override
  public String toJsonString() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("event", ExecutorStartEvent.class.getCanonicalName());
    json.put("timestamp", getTimestamp());
    json.put("eventType", getType());
    json.put("containerId", containerId);
    json.put("executorId", executorId.toString());
    json.put("executorUniqId", executorId.toUniqString());
    json.put("role", executorId.getRoleName());
    return json.toString();
  }
}
