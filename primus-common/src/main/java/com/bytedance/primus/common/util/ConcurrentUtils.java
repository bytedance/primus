/*
 * Copyright 2023 Bytedance Inc.
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

package com.bytedance.primus.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BinaryOperator;

public class ConcurrentUtils {

  // returns an empty list when callables is null or empty.
  public static <T> List<T> collect(
      ExecutorService executorService,
      List<Callable<T>> callables
  ) throws InterruptedException, ExecutionException {
    if (callables == null || callables.isEmpty()) {
      return new ArrayList<>();
    }

    // Dispatch callables
    List<T> ret = new ArrayList<>(callables.size());
    for (Future<T> future : executorService.invokeAll(callables)) {
      ret.add(future.get());
    }

    return ret;
  }

  // returns Optional.empty() when callables is null or empty.
  public static <T> Optional<T> reduce(
      ExecutorService executorService,
      List<Callable<T>> callables,
      BinaryOperator<T> accumulator
  ) throws InterruptedException, ExecutionException {
    return collect(executorService, callables).stream().reduce(accumulator);
  }
}
