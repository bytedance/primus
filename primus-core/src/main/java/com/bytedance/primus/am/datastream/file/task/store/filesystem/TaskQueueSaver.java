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
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.common.util.Sleeper;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileTaskSaver continuously polls new tasks from TaskVault.newTaskQueue and persists them to the
 * underlying storage.
 */
class TaskQueueSaver extends Thread {

  private static final Duration SLEEP_DURATION = Duration.ofMillis(3000);

  private final Logger LOG;
  private final AMContext ctx;
  private final FileSystemTaskStore owner;
  private final TaskVault vault;
  private final TaskStorage storage;

  public TaskQueueSaver(AMContext ctx, FileSystemTaskStore owner) {
    super(TaskQueueSaver.class.getName() + "[" + owner.getName() + "]");

    this.LOG = LoggerFactory.getLogger(this.getName());
    this.ctx = ctx;
    this.owner = owner;
    this.vault = owner.getTaskVault();
    this.storage = owner.getTaskStorage();

    setDaemon(true);
  }

  @Override
  public void run() {
    while (!owner.isStopped()) {
      // Start persisting to TaskFile
      boolean hasTasks = pollAndSaveTasks();
      // Sleep if no task has been saved
      if (!hasTasks) {
        LOG.debug("No new tasks, TaskSaver sleeps for {}s.", SLEEP_DURATION.getSeconds());
        Sleeper.sleepWithoutInterruptedException(SLEEP_DURATION);
      }
    }
  }

  // returns a list of saved tasks
  private boolean pollAndSaveTasks() {
    List<Task> tasks = new LinkedList<>();
    try (TimerMetric ignored = PrimusMetrics
        .getTimerContextWithAppIdTag("am.taskstore.saver.latency")
    ) {
      // collect a batch
      for (
          Optional<Task> task = vault.pollNewTask();
          task.isPresent();
          task = vault.pollNewTask()
      ) {
        tasks.add(task.get());
      }
      // persist to taskStorage
      if (tasks.isEmpty()) {
        return false;
      }
      storage
          .persistNewTasks(tasks)
          .ifPresent(result -> logSplitKey(
              result.getKey(),
              result.getValue()
          ));
    } catch (IOException e) {
      // NOTE: TaskFile could have been corrupted
      emitSaveTaskFailureTimelineEvent(e);
      owner.failApp("Failed to save new tasks onto taskStorage: e=" + e);
    }
    return true;
  }

  private void logSplitKey(int fileId, Task task) {
    String batchKey;
    if (task.getFileTask() != null) {
      batchKey = task.getFileTask().getBatchKey();
    } else {
      return;
    }

    LOG.info("Saved tasks to " + fileId
        + ", lastSavedTaskId: " + task.getTaskId()
        + ", batchKey: " + batchKey);
  }

  private void emitSaveTaskFailureTimelineEvent(Exception e) {
    JsonObject buildTaskFailed = new JsonObject();
    buildTaskFailed.addProperty("attemptId", ctx.getApplicationMeta().getAttemptId());
    buildTaskFailed.addProperty("errMsg", ExceptionUtils.getFullStackTrace(e));

    ctx.logTimelineEvent( // TODO: Standardize timeline events
        "BUILD_TASK_FAILED",
        buildTaskFailed.toString()
    );
  }
}
