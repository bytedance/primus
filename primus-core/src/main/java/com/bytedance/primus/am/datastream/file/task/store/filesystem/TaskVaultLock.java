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
import com.bytedance.primus.common.util.LockUtils;
import com.bytedance.primus.common.util.LockUtils.LockWrapper;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TaskVaultLock provides various locks for TaskVault for different scenarios to ensure
 * consistency.
 */
class TaskVaultLock {

  /**
   * Instead of serving concurrent reads and writes, a ReadWriteLock is adopted for locking the
   * driver, individual executors or all of them. The former two are implemented by acquiring the
   * read lock to allow task states to be  updated concurrently by both driver and executors.
   * Notably, fields updated during lockForDriverUpdates and lockForExecutorUpdates are required to
   * protect themselves to survive race conditions.
   * <p>
   * On the other hand, locking all is implemented by acquiring the write lock to ensure there is no
   * other ongoing mutation to task states for operations requiring the highest accuracy.
   */
  private final ReentrantReadWriteLock lockForAll = new ReentrantReadWriteLock();
  private final ReentrantLock lockForDriver = new ReentrantLock();
  private final ConcurrentHashMap<ExecutorId, ReentrantLock> locksForExecutors = new ConcurrentHashMap<>();

  protected LockWrapper lockAll() {
    return LockUtils.lock(lockForAll.writeLock());
  }

  protected LockWrapper lockForDriverUpdates() {
    return LockUtils.lock(
        lockForAll.readLock(),
        lockForDriver
    );
  }

  protected LockWrapper lockForExecutorUpdates(ExecutorId executorId) {
    return LockUtils.lock(
        lockForAll.readLock(),
        locksForExecutors.computeIfAbsent(executorId, id -> new ReentrantLock())
    );
  }
}
