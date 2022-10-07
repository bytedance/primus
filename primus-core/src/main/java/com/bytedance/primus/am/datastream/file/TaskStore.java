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

package com.bytedance.primus.am.datastream.file;

import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface TaskStore {

  void addNewTasks(List<Task> tasks);

  Task getLastSavedTask();

  int getTotalTaskNum();

  void addPendingTasks(TaskWrapper task);

  // Get all pending tasks if size <= 0 else get size tasks at best
  List<TaskWrapper> getPendingTasks(int size);

  TaskWrapper pollPendingTask();

  int getPendingTaskNum();

  void addSuccessTask(TaskWrapper task);

  Collection<TaskWrapper> getSuccessTasks();

  int getSuccessTaskNum();

  void addFailureTask(TaskWrapper task);

  Collection<TaskWrapper> getFailureTasks();

  Collection<TaskWrapper> getFailureTasksWithLimit(int limit);

  int getFailureTaskNum();

  void addExecutorRunningTasks(ExecutorId executorId, Map<Long, TaskWrapper> tasks);

  Map<Long, TaskWrapper> getRunningTasks(ExecutorId executorId);

  Map<Long, TaskWrapper> removeExecutorRunningTasks(ExecutorId executorId);

  Map<ExecutorId, Map<Long, TaskWrapper>> getExecutorRunningTaskMap();

  boolean isNoRunningTask();

  void stop();

  /**
   * Make a savepoint of state to savepointDir.
   *
   * @param savepointDir
   * @return true if succeeded else false
   */
  boolean makeSavepoint(String savepointDir);
}
