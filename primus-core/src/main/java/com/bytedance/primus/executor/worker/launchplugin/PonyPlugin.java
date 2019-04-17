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

package com.bytedance.primus.executor.worker.launchplugin;

import com.bytedance.primus.api.records.Endpoint;
import com.bytedance.primus.api.records.ExecutorSpec;
import com.bytedance.primus.api.records.ExecutorSpecs;
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.WorkerContext;
import java.util.Map;

@LaunchPlugin(value = "PonyPlugin")
public class PonyPlugin implements ChildLaunchPlugin {

  private static final String CONTAINER_ID_ENV_KEY = "CONTAINER_ID";

  private ExecutorContext executorContext;
  private WorkerContext workerContext;

  public PonyPlugin(ExecutorContext executorContext, WorkerContext workerContext) {
    this.executorContext = executorContext;
    this.workerContext = workerContext;
  }

  @Override
  public void init() throws Exception {
  }

  @Override
  public void preStart() throws Exception {
    int i = 0;
    for (Map.Entry<String, ExecutorSpecs> entry :
        workerContext.getClusterSpec().getExecutorSpecs().entrySet()) {
      for (ExecutorSpec executorSpec : entry.getValue().getExecutorSpecs()) {
        if (executorSpec == null || executorSpec.getExecutorId() == null) {
          continue;
        }
        if (executorSpec.getExecutorId().equals(executorContext.getExecutorId())) {
          for (Endpoint endpoint : executorSpec.getEndpoints()) {
            workerContext.getEnvironment().put("PORT" + i, "" + endpoint.getPort());
            i++;
          }
          break;
        }
      }
    }
    String containerId = workerContext.getEnvironment().get(CONTAINER_ID_ENV_KEY);
    String applicationAttemptId = ContainerId.fromString(containerId)
        .getApplicationAttemptId()
        .toString();
    workerContext.getEnvironment().put("PS_NAME", "/parameter_server/" + applicationAttemptId);
    workerContext.getEnvironment().put("SHARD_ID", "" + executorContext.getExecutorId().getIndex());

  }

  @Override
  public void postStart() throws Exception {
  }

  @Override
  public void preStop() throws Exception {
  }

  @Override
  public void postStop() throws Exception {
  }
}
