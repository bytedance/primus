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

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutor;
import com.bytedance.primus.am.schedulerexecutor.SchedulerExecutorStateChangeEvent;
import com.bytedance.primus.apiserver.client.models.Executor;
import com.bytedance.primus.apiserver.service.exception.ApiServerException;
import com.bytedance.primus.common.event.EventHandler;
import com.bytedance.primus.common.retry.RetryCallback;
import com.bytedance.primus.common.retry.RetryContext;
import com.bytedance.primus.common.retry.RetryTemplate;
import com.bytedance.primus.common.service.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiServerExecutorStateUpdateListener extends AbstractService implements
    EventHandler<SchedulerExecutorStateChangeEvent> {

  private static final Logger log = LoggerFactory
      .getLogger(ApiServerExecutorStateUpdateListener.class);

  private AMContext context;

  private BlockingQueue<SchedulerExecutor> retryScheduleExecutor = new LinkedBlockingQueue<>(5000);

  private Thread retryUpdaterThread;
  private boolean running = true;

  public ApiServerExecutorStateUpdateListener(AMContext context) {
    super("ApiServerStatusUpdateListenerService");
    this.context = context;
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    retryUpdaterThread = getRetryUpdaterThread();
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    if (retryUpdaterThread != null) {
      retryUpdaterThread.start();
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    if (retryUpdaterThread != null) {
      running = false;
      retryUpdaterThread.interrupt();
      retryUpdaterThread.join(TimeUnit.SECONDS.toMillis(1));
    }
  }

  public Thread getRetryUpdaterThread() {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("executor-state-updater-%d")
        .build();
    return threadFactory.newThread(() -> doRetryUpdateSchedulerStatus());
  }

  private void doRetryUpdateSchedulerStatus() {
    log.info("executor-state-updater thread started.");
    while (running) {
      try {
        SchedulerExecutor executor = retryScheduleExecutor.take();
        log.info("retry to update executor:{}, with state:{}", executor.getExecutorId(),
            executor.getExecutorState());
        try {
          updateExecutorStateToAPIServer(executor);
        } catch (ApiServerException e) {
          log.error("failed to update executor:{} with state:{}, re-enqueue executor.",
              executor.getExecutorId(),
              executor.getExecutorState());
          retryScheduleExecutor.offer(executor);
        }
      } catch (InterruptedException e) {
        log.info("received InterruptedException", e);
        break;
      }
    }
    log.info("executor-state-updater thread finished, running flag:{}", running);
  }

  @Override
  public void handle(SchedulerExecutorStateChangeEvent executorEvent) {
    switch (executorEvent.getType()) {
      case STATE_CHANGED:
        updateExecutorStateToAPIServerWithRetry(executorEvent.getSchedulerExecutor());
        break;
      default:
        log.error("SchedulerExecutorStateChangeEvent eventType:{}", executorEvent.getType());
    }
  }

  private void updateExecutorStateToAPIServerWithRetry(SchedulerExecutor schedulerExecutor) {
    RetryTemplate template = RetryTemplate.builder()
        .maxAttempts(2)
        .retryOn(Exception.class)
        .retryOn(IOException.class)
        .retryOn(ApiServerException.class)
        .backOffPeriod(TimeUnit.MILLISECONDS.toMillis(100))
        .build();
    try {
      template.execute(new RetryCallback<Boolean, ApiServerException>() {
        public Boolean doWithRetry(RetryContext context) throws ApiServerException {
          return updateExecutorStateToAPIServer(schedulerExecutor);
        }
      });
    } catch (Exception ex) {
      log.info("Failed to updateExecutorStateToAPIServerWithRetry", ex);
      boolean addToRetrySuccess = retryScheduleExecutor.offer(schedulerExecutor);
      if (!addToRetrySuccess) {
        log.warn("retrySchedulerExecutor is full, skip current event");
      }
    }
  }

  private Boolean updateExecutorStateToAPIServer(SchedulerExecutor schedulerExecutor)
      throws ApiServerException {
    String uniqId = schedulerExecutor.getExecutorId().toUniqString();
    Executor executor = context.getCoreApi().getExecutor(uniqId);
    String oldState = executor.getStatus().getState();
    String newState = schedulerExecutor.getExecutorState().toString();
    if (oldState.equalsIgnoreCase(newState)) {
      return false;
    }
    log.info("Start to updateExecutorStateToAPIServer, id:{}, old:{}, new:{}", uniqId, oldState,
        newState);
    executor.setStatus(executor.getStatus().setState(newState));
    context.getCoreApi().replaceExecutor(executor, executor.getMeta().getVersion());
    return true;
  }
}
