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

package com.bytedance.primus.utils.concurrent;

import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallableExecutionUtils<T> {

  private static final Logger log = LoggerFactory.getLogger(CallableExecutionUtils.class);

  private Callable<T> callable;
  private String threadName;
  private int timeout;
  private TimeUnit timeUnit;

  public CallableExecutionUtils(String threadName, Callable<T> callable, int timeout,
      TimeUnit timeUnit) {
    this.threadName = threadName;
    this.callable = callable;
    this.timeout = timeout;
    this.timeUnit = timeUnit;
  }

  public T doExecuteCallableSilence() {
    try {
      return doExecuteCallable(false);
    } catch (Exception e) {
      log.error("error when execute callable, name:" + threadName, e);
    }
    return null;
  }

  public T doExecuteCallable(boolean throwTimeoutException)
      throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFutureTask<T> task = ListenableFutureTask.create(callable);
    ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
    threadFactoryBuilder.setDaemon(true);
    threadFactoryBuilder.setNameFormat(threadName);
    ExecutorService executorService = Executors
        .newSingleThreadExecutor(threadFactoryBuilder.build());
    executorService.execute(task);
    try {
      return task.get(timeout, timeUnit);
    } catch (TimeoutException e) {
      if (throwTimeoutException) {
        throw e;
      }
    }
    return null;
  }

}
