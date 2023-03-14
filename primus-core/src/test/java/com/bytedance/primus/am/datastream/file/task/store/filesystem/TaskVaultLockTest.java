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

package com.bytedance.primus.am.datastream.file.task.store.filesystem;

import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.impl.pb.ExecutorIdPBImpl;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.common.util.ConcurrentUtils;
import com.bytedance.primus.common.util.LockUtils.LockWrapper;
import com.bytedance.primus.common.util.Sleeper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TaskVaultLockTest {

  static private final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(8);

  @Test
  public void testLockAll() throws ExecutionException, InterruptedException {
    TaskVaultLock lock = new TaskVaultLock();
    List<Callable<Interval>> callables = new ArrayList<Callable<Interval>>() {{
      add(newLockAllIntervalGenerator(lock));
      add(newLockAllIntervalGenerator(lock));
    }};

    List<Interval> intervals = ConcurrentUtils.collect(EXECUTOR_SERVICE, callables);
    assertAndMergeIntervals(intervals, false /* shouldOverlap */);
  }

  @Test
  public void testLockDriver() throws ExecutionException, InterruptedException {
    TaskVaultLock lock = new TaskVaultLock();
    List<Callable<Interval>> callables = new ArrayList<Callable<Interval>>() {{
      add(newLockDriverIntervalGenerator(lock));
      add(newLockDriverIntervalGenerator(lock));
    }};

    List<Interval> intervals = ConcurrentUtils.collect(EXECUTOR_SERVICE, callables);
    assertAndMergeIntervals(intervals, false /* shouldOverlap */);
  }

  @Test
  public void testLockSameExecutor() throws ExecutionException, InterruptedException {
    TaskVaultLock lock = new TaskVaultLock();
    List<Callable<Interval>> callables = new ArrayList<Callable<Interval>>() {{
      add(newLockExecutorIntervalGenerator(lock, 0));
      add(newLockExecutorIntervalGenerator(lock, 0));
    }};

    List<Interval> intervals = ConcurrentUtils.collect(EXECUTOR_SERVICE, callables);
    assertAndMergeIntervals(intervals, false /* shouldOverlap */);
  }

  @Test
  public void testLockDifferentExecutors() throws ExecutionException, InterruptedException {
    TaskVaultLock lock = new TaskVaultLock();
    List<Callable<Interval>> callables = new ArrayList<Callable<Interval>>() {{
      add(newLockExecutorIntervalGenerator(lock, 0));
      add(newLockExecutorIntervalGenerator(lock, 1));
      add(newLockExecutorIntervalGenerator(lock, 2));
    }};

    List<Interval> intervals = ConcurrentUtils.collect(EXECUTOR_SERVICE, callables);
    assertAndMergeIntervals(intervals, true /* shouldOverlap */);
  }

  @Test
  public void testLockAllThenLockDifferentUpdates()
      throws ExecutionException, InterruptedException {

    TaskVaultLock lock = new TaskVaultLock();
    List<Callable<Interval>> callables = new ArrayList<Callable<Interval>>() {{
      add(newLockAllIntervalGenerator(lock));
      add(newLockDriverIntervalGenerator(lock));
      add(newLockExecutorIntervalGenerator(lock, 0));
      add(newLockExecutorIntervalGenerator(lock, 1));
      add(newLockExecutorIntervalGenerator(lock, 2));
    }};

    List<Interval> intervals = ConcurrentUtils.collect(EXECUTOR_SERVICE, callables);
    assertAndMergeIntervals(
        new ArrayList<Interval>() {{
          add(intervals.get(0));
          add(assertAndMergeIntervals(intervals.subList(1, 5), true /* shouldOverlap */));
        }},
        false /* shouldOverlap */);
  }

  @Test
  public void testLockDifferentUpdatesThenLockAll()
      throws ExecutionException, InterruptedException {

    TaskVaultLock lock = new TaskVaultLock();
    List<Callable<Interval>> callables = new ArrayList<Callable<Interval>>() {{
      add(newLockDriverIntervalGenerator(lock));
      add(newLockExecutorIntervalGenerator(lock, 0));
      add(newLockExecutorIntervalGenerator(lock, 1));
      add(newLockExecutorIntervalGenerator(lock, 2));
      add(newLockAllIntervalGenerator(lock));
    }};

    List<Interval> intervals = ConcurrentUtils.collect(EXECUTOR_SERVICE, callables);
    assertAndMergeIntervals(
        new ArrayList<Interval>() {{
          add(assertAndMergeIntervals(intervals.subList(0, 4), true /* shouldOverlap */));
          add(intervals.get(4));
        }},
        false /* shouldOverlap */);
  }

  private Callable<Interval> newLockAllIntervalGenerator(TaskVaultLock lock) {
    return () -> {
      try (LockWrapper ignored = lock.lockAll()) {
        long start = System.nanoTime();
        Sleeper.sleep(Duration.ofSeconds(1));
        return new Interval(start, System.nanoTime());
      }
    };
  }

  private Callable<Interval> newLockDriverIntervalGenerator(TaskVaultLock lock) {
    return () -> {
      try (LockWrapper ignored = lock.lockForDriverUpdates()) {
        long start = System.nanoTime();
        Sleeper.sleep(Duration.ofSeconds(1));
        return new Interval(start, System.nanoTime());
      }
    };
  }

  private Callable<Interval> newLockExecutorIntervalGenerator(TaskVaultLock lock, int token) {
    ExecutorId executorId = new ExecutorIdPBImpl();
    executorId.setRoleName(String.valueOf(token));
    executorId.setIndex(token);
    executorId.setUniqId(System.currentTimeMillis());

    return () -> {
      try (LockWrapper ignored = lock.lockForExecutorUpdates(executorId)) {
        long start = System.nanoTime();
        Sleeper.sleep(Duration.ofSeconds(1));
        return new Interval(start, System.nanoTime());
      }
    };
  }

  private Interval assertAndMergeIntervals(List<Interval> intervals, boolean shouldOverlap) {
    return intervals.stream()
        .reduce((acc, cur) -> {
          Assertions.assertEquals(
              shouldOverlap,
              acc.overlapped(cur),
              String.format("should overlapped: %b, acc: %s, cur: %s", shouldOverlap, acc, cur)
          );
          return acc.merge(cur);
        })
        .orElse(null);
  }

  // half-opened interval
  private static class Interval extends Pair<Long, Long> {

    public Interval(Long start, Long end) {
      super(start, end);
    }

    public long getStart() {
      return getKey();
    }

    public long getEnd() {
      return getValue();
    }

    public Interval merge(Interval other) {
      return new Interval(
          Long.min(getStart(), other.getStart()),
          Long.max(getEnd(), other.getEnd())
      );
    }

    public boolean overlapped(Interval other) {
      return getStart() < other.getEnd() && getEnd() > other.getStart();
    }
  }
}
