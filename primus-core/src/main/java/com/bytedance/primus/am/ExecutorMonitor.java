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

package com.bytedance.primus.am;

import static com.bytedance.primus.utils.PrimusConstants.UPDATE_TO_API_SERVER_RETRY_INTERVAL_MS;
import static com.bytedance.primus.utils.PrimusConstants.UPDATE_TO_API_SERVER_RETRY_TIMES;

import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEvent;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorManagerEventType;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.model.records.Container;
import com.bytedance.primus.common.util.AbstractLivelinessMonitor;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.common.util.UTCClock;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorMonitor extends AbstractLivelinessMonitor<ExecutorId> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorMonitor.class);
  private AMContext context;
  private int expireInterval;
  private volatile boolean isStopped;
  private Thread executorStatusUpdater;

  public ExecutorMonitor(AMContext context) {
    super(ExecutorMonitor.class.getName(), new UTCClock());
    this.context = context;
    isStopped = false;
    executorStatusUpdater = new ExecutorStatusUpdater();
    executorStatusUpdater.setDaemon(true);
  }

  public void serviceInit(Configuration conf) {
    expireInterval = context.getPrimusConf().getScheduler().getHeartbeatIntervalMs() *
        context.getPrimusConf().getScheduler().getMaxMissedHeartbeat();
    setExpireInterval(expireInterval);
    setMonitorInterval(expireInterval);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    executorStatusUpdater.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    isStopped = true;
    super.serviceStop();
  }

  @Override
  protected void expire(ExecutorId executorId) {
    context.getDispatcher().getEventHandler()
        .handle(new SchedulerExecutorManagerEvent(
            SchedulerExecutorManagerEventType.EXECUTOR_EXPIRED, executorId));
  }

  class ExecutorStatusUpdater extends Thread {

    @Override
    public void run() {
      while (!isStopped) {
        Sleeper.sleepWithoutInterruptedException(Duration.ofMillis(
            30L * context.getPrimusConf().getScheduler().getHeartbeatIntervalMs()));

        for (SchedulerExecutor se :
            context.getSchedulerExecutorManager().getRunningExecutorMap().values()) {
          try {
            updateExecutorToApiServer(se.getExecutorId(), se.getContainer());
          } catch (Exception e) {
            LOG.warn("Failed to update executor metrics to api server", e);
          }
        }
      }
    }
  }

  private void updateExecutorToApiServer(
      ExecutorId executorId,
      Container container
  ) throws Exception {
    if (!context.needToUpdateExecutorToApiServer()) {
      return;
    }

    int retryTimes = 0;
    while (true) {
      try {
        // TODO: API server is visited anyway, maybe we can always send the new status to API server
        //  and let it decide how to handle them. Also, a write-after-read pattern can be removed.
        Executor executor = context.getCoreApi().getExecutor(executorId.toUniqString());
        Map<String, String> oldMetrics = executor.getStatus().getMetrics();
        Map<String, String> newMetrics = context.retrieveContainerMetric(container);
        if (isMetricsChanged(oldMetrics, newMetrics)) {
          executor.getStatus().setMetrics(newMetrics);
          context.getCoreApi().replaceExecutor(executor, executor.getMeta().getVersion());
        }
        return;
      } catch (StatusRuntimeException | PrimusRuntimeException | IOException | ApiServerException
          runtimeException) {
        if (++retryTimes >= UPDATE_TO_API_SERVER_RETRY_TIMES) {
          throw runtimeException;
        }
      }

      Sleeper.sleepWithoutInterruptedException(
          Duration.ofMillis(UPDATE_TO_API_SERVER_RETRY_INTERVAL_MS));
    }
  }

  protected boolean isMetricsChanged(Map<String, String> existedMetricsMap,
      Map<String, String> newMetricsMap) {
    return !existedMetricsMap.equals(newMetricsMap);
  }
}
