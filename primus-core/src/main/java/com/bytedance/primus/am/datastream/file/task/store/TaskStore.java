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

package com.bytedance.primus.am.datastream.file.task.store;

import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskStatus;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TaskStore {

  void stop();

  boolean takeSnapshot(String directory);

  /**
   * One of the most important missions of TaskStore is tracking the state of each single task;
   * hence, TaskStore provides a set of methods allowing callers to update the task states
   * correspondingly. Notably, these methods are mutable.
   */

  void addNewTasks(List<Task> tasks);

  Map<Long, TaskWrapper> updateAndRemoveTasks(
      ExecutorId executorId,
      Map<Long, TaskStatus> taskStatusMapFromExecutor,
      int taskAttemptLimit,
      int taskAttemptModifier
  );

  Map<Long, TaskWrapper> updateAndAssignTasks(
      ExecutorId executorId,
      Map<Long, TaskStatus> taskStatusMapFromExecutor,
      int taskAttemptLimit,
      int maxTaskNumPerWorker,
      boolean needMoreTask
  );

  /**
   * Besides, states and statistics are served for management purposes via immutable functions.
   */

  boolean hasBeenExhausted();

  boolean hasRunningTasks();

  TaskStatistics getTaskStatistics();

  Optional<Task> getLastSavedTask();

  Map<Long, TaskWrapper> getRunningTasks();

  // Capped by limit if positive, where retrieve all is not supported
  Collection<TaskWrapper> getPendingTasks(int limit);

  // Capped by limit if positive, else retrieve all with best effort
  Collection<TaskWrapper> getSuccessTasks(int limit);

  // Capped by limit if positive, else retrieve all with best effort
  Collection<TaskWrapper> getFailureTasks(int limit);
}
