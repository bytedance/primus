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
import com.bytedance.primus.executor.worker.WorkerContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ExecutorRegisteredEvent extends ExecutorEvent {

  private WorkerContext workerContext;

  public ExecutorRegisteredEvent(WorkerContext workerContext, ExecutorId excutorId) {
    super(ExecutorEventType.REGISTERED, excutorId);
    this.workerContext = workerContext;
  }

  WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public String toJsonString() throws JSONException {
    JSONObject json = new JSONObject();
    if (executorId != null) {
      json.put("executor", executorId.toUniqString());
    }
    json.put("command", workerContext.getCommand());
    json.put("env", workerContext.getEnvironment());
    return json.toString();
  }
}
