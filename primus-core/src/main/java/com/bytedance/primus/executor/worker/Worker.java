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

package com.bytedance.primus.executor.worker;

import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY;

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.common.child.Child;
import com.bytedance.primus.common.child.ChildContext;
import com.bytedance.primus.common.child.ChildEvent;
import com.bytedance.primus.common.child.ChildEventType;
import com.bytedance.primus.common.child.ChildExitedEvent;
import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.common.child.ChildStartedEvent;
import com.bytedance.primus.common.event.Dispatcher;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.ExecutorEvent;
import com.bytedance.primus.executor.ExecutorEventType;
import com.bytedance.primus.executor.ExecutorExitCode;
import com.bytedance.primus.executor.ExecutorFailedEvent;
import com.bytedance.primus.executor.task.WorkerStartEvent;
import com.bytedance.primus.executor.worker.launchplugin.FifoPlugin;
import com.bytedance.primus.executor.worker.launchplugin.WorkerLaunchPluginManager;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Child {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private static final int BUFFER_SIZE_BYTES = 128 * 1024;

  private ExecutorContext executorContext;
  private WorkerContext workerContext;
  private ChildLaunchPlugin launchPlugin;
  private Dispatcher dispatcher;
  private ExecutorId executorId;

  public Worker(ExecutorContext executorContext, WorkerContext workerContext,
      Dispatcher dispatcher) {
    this.executorContext = executorContext;
    this.workerContext = workerContext;
    this.launchPlugin =
        WorkerLaunchPluginManager.getWorkerLaunchPluginChain(executorContext, workerContext);
    this.dispatcher = dispatcher;
    executorId = executorContext.getExecutorId();
  }

  @Override
  public ChildContext getContext() {
    return workerContext;
  }

  @Override
  public ChildLaunchPlugin getLaunchPlugin() {
    return launchPlugin;
  }

  @Override
  public void handle(ChildEvent event) {
    switch (event.getType()) {
      case LAUNCH_FAILED:
        dispatcher.getEventHandler().handle(
            new ExecutorFailedEvent(ExecutorExitCode.LAUNCH_FAIL.getValue(), event.getMessage(),
                executorId));
        break;
      case CHILD_STARTED:
        try {
          switch (executorContext.getPrimusExecutorConf().getPrimusConf().getChannelConfigCase()) {
            case FIFO_PIPE:
              Map<String, String> env = workerContext.getEnvironment();
              String fifoName = env.get(FifoPlugin.FIFO_ENV_KEY);
              OutputStream output =
                  new BufferedOutputStream(new FileOutputStream(fifoName), BUFFER_SIZE_BYTES);
              dispatcher.getEventHandler().handle(new WorkerStartEvent(output));
              break;
            default:
              OutputStream outputStream = ((ChildStartedEvent) event).getOutputStream();
              dispatcher.getEventHandler().handle(new WorkerStartEvent(outputStream));
              break;
          }
          dispatcher.getEventHandler()
              .handle(new ExecutorEvent(ExecutorEventType.STARTED, executorId));
          long submitTime = 0;
          if (workerContext.getEnvironment().containsKey(PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY)) {
            submitTime =
                Long.valueOf(workerContext.getEnvironment().get(PRIMUS_SUBMIT_TIMESTAMP_ENV_KEY));
          }
          PrimusMetrics.emitStoreWithOptionalPrefix(
              "executor.worker_launch.submit_running.interval_ms{role="
                  + executorContext.getExecutorId().getRoleName() + "}",
              (int) (new Date().getTime() - submitTime));
        } catch (FileNotFoundException e) {
          handle(new ChildEvent(ChildEventType.LAUNCH_FAILED, e.getMessage()));
        }
        break;
      case CHILD_INTERRUPTED:
        dispatcher.getEventHandler()
            .handle(new ExecutorFailedEvent(ExecutorExitCode.WORKER_INTERRUPTED.getValue(),
                event.getMessage(), executorId));
      case CHILD_EXITED:
        int exitCode = ((ChildExitedEvent) event).getExitCode();
        if (exitCode == 0) {
          dispatcher.getEventHandler()
              .handle(new ExecutorEvent(ExecutorEventType.SUCCEEDED, executorId));
        } else if (exitCode == ExecutorExitCode.KILLED.getValue()) {
          dispatcher.getEventHandler()
              .handle(new ExecutorEvent(ExecutorEventType.KILLED, executorId));
        } else {
          dispatcher.getEventHandler()
              .handle(new ExecutorFailedEvent(exitCode, buildExitMessage(exitCode), executorId));
        }
        break;
    }
  }

  private String buildExitMessage(int exitCode) {
    String exitMessage = "Worker failed";
    if (isCoreDump(exitCode)) {
      exitMessage = "Worker core dumped";
      PrimusMetrics.emitCounterWithOptionalPrefix(
          "executor.worker_launch.core_dump{role="
              + executorId.getRoleName() + "}", 1);
    }
    return exitMessage;
  }

  private boolean isCoreDump(int exitCode) {
    // See man signal in linux for details
    return (exitCode >= 128 + 3 && exitCode <= 128 + 8)
        || (exitCode >= 128 + 10 && exitCode <= 128 + 12);
  }

  public ExecutorContext getExecutorContext() {
    return executorContext;
  }
}
