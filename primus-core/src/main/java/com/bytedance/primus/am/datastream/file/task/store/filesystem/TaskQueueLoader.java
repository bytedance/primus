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
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.common.util.Sleeper;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Duration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaskQueueLoader continuously reads tasks from TaskStorage and inserts them into
 * TaskVault.newPendingTaskQueue.
 */
class TaskQueueLoader extends Thread {

  private static final Duration SLEEP_DURATION = Duration.ofMillis(3000);

  private final Logger LOG;
  private final AMContext ctx;
  private final FileSystemTaskStore owner;
  private final TaskVault vault;
  private final TaskStorage storage;

  public TaskQueueLoader(FileSystemTaskStore owner) {
    super(TaskQueueLoader.class.getName() + "[" + owner.getName() + "]");

    this.LOG = LoggerFactory.getLogger(this.getName());
    this.ctx = owner.getContext();
    this.owner = owner;
    this.vault = owner.getTaskVault();
    this.storage = owner.getTaskStorage();

    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      // Wait for TaskStore to be ready
      if (!shouldKeepWorking()) {
        return;
      }

      // Initialization
      Pair<Integer, Long> index = storage.getFileIdAndPosition(vault.getMaxLoadedTaskId() + 1);
      int fileId = index.getKey();
      long position = index.getValue();

      // Start working
      LOG.info("start loading tasks from fileId: {}, position: {}", fileId, position);
      while (shouldKeepWorking()) {
        storage.loadNewTasksToVault(fileId, position, vault);
        // Move on to next iteration
        fileId += 1;
        position = 0;
      }

    } catch (IOException e) {
      emitLoadTaskFailureTimelineEvent(e);
      owner.failApp("Failed to load tasks onto taskStorage: e=" + e);
    }

    LOG.info("Stopped loading tasks from TaskStorage");
  }

  // Wait for tasks and returns whether it's time to stop working.
  private boolean shouldKeepWorking() {
    while (true) {
      if (owner.isStopped()) {
        return false;
      }
      if (storage.hasTasks() && storage.getMaxTaskId() > vault.getMaxLoadedTaskId()) {
        return true;
      }
      Sleeper.sleepWithoutInterruptedException(SLEEP_DURATION);
    }
  }

  private void emitLoadTaskFailureTimelineEvent(Exception e) {
    JsonObject buildTaskFailed = new JsonObject();
    buildTaskFailed.addProperty("attemptId", ctx.getApplicationMeta().getAttemptId());
    buildTaskFailed.addProperty("errMsg", ExceptionUtils.getFullStackTrace(e));

    ctx.logTimelineEvent( // TODO: Standardize timeline events
        "BUILD_TASK_FAILED",
        buildTaskFailed.toString()
    );
  }
}
