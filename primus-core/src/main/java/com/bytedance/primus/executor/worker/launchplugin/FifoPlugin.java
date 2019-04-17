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

import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.ExecutorExitCode;
import com.bytedance.primus.executor.worker.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FifoPlugin implements ChildLaunchPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(FifoPlugin.class);
  public static final String FIFO_ENV_KEY = "FIFO_NAME";
  private static final String FIFO_NAME_SUFFIX = "_fifo";

  private ExecutorContext executorContext;
  private WorkerContext workerContext;
  private Map<String, String> envs;

  public FifoPlugin(ExecutorContext executorContext, WorkerContext workerContext) {
    this.executorContext = executorContext;
    this.workerContext = workerContext;
    this.envs = new HashMap<>();
    String fifoName = executorContext.getExecutorId().toString() + FIFO_NAME_SUFFIX;
    envs.put(FIFO_ENV_KEY, fifoName);
    String[] command = new String[]{"/usr/bin/mkfifo", fifoName};
    Process mkfifoProcess = null;
    try {
      mkfifoProcess = new ProcessBuilder(command).inheritIO().start();
      mkfifoProcess.waitFor();
    } catch (IOException e) {
      System.exit(ExecutorExitCode.MKFIFO_FAIL.getValue());
    } catch (InterruptedException e) {
    }
    LOG.info("Executor mkfifo " + fifoName + " ok");
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
