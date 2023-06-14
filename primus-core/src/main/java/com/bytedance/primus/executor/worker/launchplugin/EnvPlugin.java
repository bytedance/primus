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

import static com.bytedance.primus.apiserver.utils.Constants.API_SERVER_RPC_HOST_ENV;
import static com.bytedance.primus.apiserver.utils.Constants.API_SERVER_RPC_PORT_ENV;

import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.WorkerContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class EnvPlugin implements ChildLaunchPlugin {

  private static final String PRIMUS_EXECUTOR_ID_ENV_KEY = "PRIMUS_EXECUTOR_ID";
  private static final String PRIMUS_EXECUTOR_ROLE_INDEX_ENV_KEY = "PRIMUS_EXECUTOR_ROLE_INDEX";
  private static final String PRIMUS_EXECUTOR_RESERVED_PORTS_ENV_KEY = "PRIMUS_EXECUTOR_RESERVED_PORTS";

  private final WorkerContext workerContext;
  private final Map<String, String> envs = new HashMap<>();

  public EnvPlugin(
      ExecutorContext executorContext,
      WorkerContext workerContext
  ) {
    this.workerContext = workerContext;

    envs.put(
        PRIMUS_EXECUTOR_ID_ENV_KEY,
        String.valueOf(executorContext.getExecutorId().getUniqId())
    );
    envs.put(
        PRIMUS_EXECUTOR_ROLE_INDEX_ENV_KEY,
        String.valueOf(executorContext.getExecutorId().getIndex())
    );

    if (!executorContext.getPrimusExecutorConf().getPortList().isEmpty()) {
      envs.put(
          PRIMUS_EXECUTOR_RESERVED_PORTS_ENV_KEY,
          StringUtils.join(executorContext.getPrimusExecutorConf().getPortList(), ",")
      );
    }

    envs.put(
        API_SERVER_RPC_HOST_ENV,
        executorContext.getPrimusExecutorConf().getApiServerHost()
    );
    envs.put(
        API_SERVER_RPC_PORT_ENV,
        String.valueOf(executorContext.getPrimusExecutorConf().getApiServerPort())
    );
  }

  @Override
  public void init() throws Exception {
  }

  @Override
  public void preStart() throws Exception {
    workerContext.getEnvironment().putAll(envs);
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
