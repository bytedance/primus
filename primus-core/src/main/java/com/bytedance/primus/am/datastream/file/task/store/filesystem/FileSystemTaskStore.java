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

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.datastream.file.task.store.TaskStatistics;
import com.bytedance.primus.am.datastream.file.task.store.TaskStore;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FileTaskStore persists and loads generated tasks from the underlying taskStorage.
public class FileSystemTaskStore implements TaskStore {

  private final Logger LOG;
  @Getter
  private final AMContext context;
  @Getter
  private final String name;

  // TODO(hopang): remove the getters
  @Getter
  private final TaskVault taskVault;
  private final TaskStorage taskStorage;

  private volatile boolean isStopped = false;

  private final TaskQueueSaver taskQueueSaver;
  private final TaskQueueLoader taskQueueLoader;
  private final TaskPreserver taskPreserver;

  public FileSystemTaskStore(
      AMContext context,
      String name,
      String savepointDir
  ) throws IOException {

    this.LOG = LoggerFactory.getLogger(FileSystemTaskStore.class.getName() + "[" + name + "]");
    this.context = context;
    this.name = name;

    // Recover from the underlying taskStorage
    TimerMetric recoverLatency = PrimusMetrics.getTimerContextWithAppIdTag(
        "am.taskstore.recover.cost", "name", name);

    this.taskStorage = TaskStorage.create(this, savepointDir);
    this.taskVault = new TaskVault(this, this.taskStorage);

    taskQueueSaver = new TaskQueueSaver(context, this);
    taskQueueLoader = new TaskQueueLoader(this);
    taskPreserver = new TaskPreserver(context, this);

    taskPreserver.start();
    taskQueueLoader.start();
    taskQueueSaver.start();

    long latency = recoverLatency.stop();
    LOG.info("Recovery elapsed " + latency + " ms");

    TaskStatistics taskStatistics = taskVault.getTaskStatistics();
    LOG.info("lastSavedTask: " + taskStorage.getLastSavedTask()
        + ", lastLoadedTaskId: " + taskVault.getMaxLoadedTaskId()
        + ", maxSuccessTaskId " + taskVault.getMaxSuccessTaskId()
        + ", maxFailureTaskId " + taskVault.getMaxFailureTaskId()
        + ", totalTaskNum " + taskStatistics.getTotalTaskNum()
        + ", pendingTaskNum " + taskStatistics.getPendingTaskNum()
        + ", successTaskNum " + taskStatistics.getSuccessTaskNum()
        + ", failureTaskNum " + taskStatistics.getFailureTaskNum()
    );
  }

  public boolean isStopped() {
    return isStopped;
  }

  public TaskStorage getTaskStorage() {
    return taskStorage;
  }

  @Override
  public TaskStatistics getTaskStatistics() {
    return taskVault.getTaskStatistics();
  }

  @Override
  public Optional<Task> getLastSavedTask() {
    return taskStorage.getLastSavedTask();
  }

  @Override
  public List<TaskWrapper> getPendingTasks(int size) {
    return taskVault.peekPendingTasks(size);
  }

  // Task Operations ===============================================================================
  // ===============================================================================================

  @Override
  public void addNewTasks(List<Task> tasks) {
    taskVault.addNewTasks(tasks);
  }

  @Override
  public Map<Long, TaskWrapper> updateAndRemoveTasks(
      ExecutorId executorId,
      Map<Long, TaskStatus> taskStatusMapFromExecutor,
      int taskAttemptLimit,
      int taskAttemptModifier
  ) {
    return taskVault.updateAndRemoveTasks(
        executorId,
        taskStatusMapFromExecutor,
        taskAttemptLimit,
        taskAttemptModifier
    );
  }

  @Override
  public Map<Long, TaskWrapper> updateAndAssignTasks(
      ExecutorId executorId,
      Map<Long, TaskStatus> taskStatusMapFromExecutor,
      int taskAttemptLimit,
      int maxTaskNumPerWorker,
      boolean needMoreTask
  ) {
    return taskVault.updateAndAssignTasks(
        executorId,
        taskStatusMapFromExecutor,
        taskAttemptLimit,
        maxTaskNumPerWorker,
        needMoreTask
    );
  }

  @Override
  public boolean hasBeenExhausted() {
    return taskVault.hasBeenExhausted();
  }

  @Override
  public boolean hasRunningTasks() {
    return taskVault.hasRunningTask();
  }

  @Override
  public Collection<TaskWrapper> getSuccessTasks(int limit) {
    // Collect from in-memory tasks first.
    List<TaskWrapper> ret = new LinkedList<>(taskVault.peekSuccessTasks(limit));
    if (ret.size() <= limit) {
      return ret;
    }
    // Supplement from taskStorage.
    ret.addAll(taskStorage.getSuccessTasksFromFs(limit - ret.size()));
    return ret;
  }

  @Override
  public Collection<TaskWrapper> getFailureTasks(int limit) {
    // Collect from in-memory tasks first.
    List<TaskWrapper> ret = new LinkedList<>(taskVault.peekFailureTasks(limit));
    if (ret.size() <= limit) {
      return ret;
    }
    // Supplement from taskStorage.
    ret.addAll(taskStorage.getFailureTasksFromFs(limit - ret.size()));
    return ret;
  }

  @Override
  public Map<Long, TaskWrapper> getRunningTasks() {
    return taskVault.peekRunningTasks();
  }

  @Override
  public void stop() {
    isStopped = true;
    taskPreserver.interrupt();
    taskQueueLoader.interrupt();
    taskQueueSaver.interrupt();
    try {
      taskPreserver.join();
      LOG.info("task preserver stopped");
      taskQueueLoader.join();
      LOG.info("task loader stopped");
      taskQueueSaver.join();
      LOG.info("task saver stopped");
    } catch (InterruptedException e) {
      LOG.warn("Stopping caught interrupted exception", e);
    }
  }

  public void failApp(String reason) {
    LOG.error("Master is failed for " + reason);
    context.emitFailAttemptEvent(
        reason,
        ApplicationExitCode.MASTER_FAILED.getValue()
    );
  }

  @Override
  public boolean takeSnapshot(String directory) {
    try {
      LOG.info("Start making a savepoint under: {}", directory);
      Path snapshotPath = new Path(directory, name);
      taskStorage.snapshot(snapshotPath);
      LOG.info("Successfully made a savepoint under {}", directory);
      return true;

    } catch (IOException e) {
      LOG.error("Failed to made a savepoint under {}, err: {}", directory, e);
      return false;
    }
  }
}
