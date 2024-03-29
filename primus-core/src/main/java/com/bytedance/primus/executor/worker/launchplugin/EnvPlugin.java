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
import static com.bytedance.primus.apiserver.utils.Constants.PRIMUS_EXECUTOR_UNIQID_ENV;

import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.WorkerContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class EnvPlugin implements ChildLaunchPlugin {

  private static final String EXECUTOR_ID_ENV_KEY = "EXECUTOR_ID";
  private static final String PORT_LIST_ENV_KEY = "PORT_LIST";

  private ExecutorContext executorContext;
  private WorkerContext workerContext;
  private Map<String, String> envs;

  public EnvPlugin(ExecutorContext executorContext, WorkerContext workerContext) {
    this.executorContext = executorContext;
    this.workerContext = workerContext;
    this.envs = new HashMap<>();
    envs.put(EXECUTOR_ID_ENV_KEY, String.valueOf(executorContext.getExecutorId().getIndex()));
    if (executorContext.getPrimusExecutorConf().getPortList().size() != 0) {
      envs.put(PORT_LIST_ENV_KEY,
          StringUtils.join(executorContext.getPrimusExecutorConf().getPortList(), ","));
    }

    // Trick and temporary solution to fix bug of coredump_handler.
    // Overwrite NM_AUX_SERVICE_mapreduce_shuffle environment because its value contains
    // a '\0' and causes coredump_handler can not parse CORE_DUMP_PROC_NAME environment.
    envs.put("NM_AUX_SERVICE_mapreduce_shuffle", "USELESS_OVERWRITE");

    envs.put(API_SERVER_RPC_HOST_ENV, executorContext.getPrimusExecutorConf().getApiServerHost());
    envs.put(API_SERVER_RPC_PORT_ENV,
        String.valueOf(executorContext.getPrimusExecutorConf().getApiServerPort()));
    envs.put(PRIMUS_EXECUTOR_UNIQID_ENV, executorContext.getExecutorId().toUniqString());
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
