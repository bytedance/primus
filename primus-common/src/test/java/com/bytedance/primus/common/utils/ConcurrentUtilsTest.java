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

package com.bytedance.primus.common.utils;

import com.bytedance.primus.common.util.ConcurrentUtils;
import com.bytedance.primus.common.util.Sleeper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentUtilsTest {

  static private final ExecutorService executorService = Executors.newFixedThreadPool(4);
  static private final List<Integer> source = IntStream
      .range(0, 8)
      .boxed()
      .collect(Collectors.toList());

  @Test
  public void testCollectWithEmpty() throws ExecutionException, InterruptedException {
    Assert.assertEquals(
        new ArrayList<>(),
        ConcurrentUtils.collect(
            executorService,
            null
        ));

    Assert.assertEquals(
        new ArrayList<>(),
        ConcurrentUtils.collect(
            executorService,
            new ArrayList<>()
        ));
  }

  @Test
  public void testCollect() throws ExecutionException, InterruptedException {
    List<Callable<Integer>> callables = source.stream()
        .map(value -> (Callable<Integer>) () -> {
          Sleeper.sleep(Duration.ofMillis((value % 2) * 100)); // Zigzag sleep time
          return value;
        })
        .collect(Collectors.toList());

    Assert.assertEquals(
        source,
        ConcurrentUtils.collect(executorService, callables)
    );
  }

  @Test
  public void testReduceWithEmpty() throws ExecutionException, InterruptedException {
    Assert.assertEquals(
        Optional.empty(),
        ConcurrentUtils.reduce(
            executorService,
            null,
            (a, b) -> a
        ));

    Assert.assertEquals(
        Optional.empty(),
        ConcurrentUtils.reduce(
            executorService,
            new ArrayList<>(),
            (a, b) -> a
        ));
  }

  @Test
  public void testReduce() throws ExecutionException, InterruptedException {
    List<Callable<Integer>> callables = source.stream()
        .map(value -> (Callable<Integer>) () -> {
          Sleeper.sleep(Duration.ofMillis((value % 2) * 100)); // Zigzag sleep time
          return value;
        })
        .collect(Collectors.toList());

    Assert.assertEquals(
        Optional.of(28),
        ConcurrentUtils.reduce(
            executorService,
            callables,
            Integer::sum
        )
    );
  }
}
