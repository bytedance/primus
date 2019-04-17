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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class CallableExecutionUtilsBuilder<T> {

  private String threadName;
  private Callable<T> callable;
  private int timeout;
  private TimeUnit timeUnit;

  public CallableExecutionUtilsBuilder setThreadName(String threadName) {
    this.threadName = threadName;
    return this;
  }

  public CallableExecutionUtilsBuilder setCallable(Callable<T> callable) {
    this.callable = callable;
    return this;
  }

  public CallableExecutionUtilsBuilder setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public CallableExecutionUtilsBuilder setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
    return this;
  }

  public CallableExecutionUtils createCallableExecutionUtils() {
    return new CallableExecutionUtils(threadName, callable, timeout, timeUnit);
  }
}