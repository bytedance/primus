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

import com.bytedance.primus.api.records.ExecutorId;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ExecutorFailedEvent extends ExecutorEvent {

  private int exitCode;
  private String exitMessage;

  public ExecutorFailedEvent(int exitCode, String exitMessage, ExecutorId executorId) {
    super(ExecutorEventType.FAILED, executorId);
    this.exitCode = exitCode;
    this.exitMessage = exitMessage;
  }

  public int getExitCode() {
    return exitCode;
  }

  public String getExitMessage() {
    return exitMessage;
  }


  @Override
  public String toJsonString() throws JSONException {
    JSONObject json = new JSONObject();
    if (executorId != null) {
      json.put("executor", executorId.toUniqString());
    }
    json.put("exitCode", exitCode);
    json.put("exitMsg", exitMessage);
    return json.toString();
  }
}
