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
import com.bytedance.primus.executor.worker.WorkerContext;
import com.bytedance.primus.utils.concurrent.CallableExecutionUtils;
import com.bytedance.primus.utils.concurrent.CallableExecutionUtilsBuilder;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class EofPlugin implements ChildLaunchPlugin {

  private ExecutorContext executorContext;

  public EofPlugin(ExecutorContext executorContext, WorkerContext workerContext) {
    this.executorContext = executorContext;
  }

  @Override
  public void init() throws Exception {

  }

  @Override
  public void preStart() throws Exception {

  }

  @Override
  public void postStart() throws Exception {

  }

  @Override
  public void preStop() throws Exception {
    Duration gracefulShutdownTimeoutDuration = executorContext.getGracefulShutdownDuration();
    Callable<Boolean> task = () -> {
      executorContext.getWorkerFeeder().close();
      executorContext.getChildLauncher().waitFor(
          gracefulShutdownTimeoutDuration.getSeconds(),
          TimeUnit.SECONDS);
      return true;
    };
    CallableExecutionUtils callableExecutionUtils = new CallableExecutionUtilsBuilder<Boolean>()
        .setCallable(task)
        .setThreadName("EofPluginPreStopThread")
        .setTimeout((int) gracefulShutdownTimeoutDuration.getSeconds())
        .setTimeUnit(TimeUnit.SECONDS)
        .createCallableExecutionUtils();
    callableExecutionUtils.doExecuteCallable(false);
  }

  @Override
  public void postStop() throws Exception {

  }
}
