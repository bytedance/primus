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
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.common.util.IntegerUtils;
import com.bytedance.primus.common.util.Sleeper;
import java.io.IOException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TaskPreserver extends Thread {

  private final Logger LOG;
  private final AMContext ctx;
  private final FileSystemTaskStore store;
  private final TaskVault vault;
  private final TaskStorage storage;

  private final Duration dumpInterval;

  public TaskPreserver(AMContext ctx, FileSystemTaskStore store) {
    super(TaskPreserver.class.getName() + "[" + store.getName() + "]");

    this.LOG = LoggerFactory.getLogger(this.getName());
    this.ctx = ctx;
    this.store = store;
    this.vault = store.getTaskVault();
    this.storage = store.getTaskStorage();

    dumpInterval = Duration.ofSeconds(
        IntegerUtils.ensurePositiveOrDefault(
            ctx.getApplicationMeta()
                .getPrimusConf()
                .getInputManager()
                .getWorkPreserve()
                .getDumpIntervalSec(),
            300
        ));

    setDaemon(true);
  }

  @Override
  public void run() {
    while (!store.isStopped()) {
      try {
        // Create an in-memory copy to avoid big critical sections.
        LOG.info("Creating checkpoint");
        Checkpoint checkpoint;
        try (TimerMetric ignored = PrimusMetrics
            .getTimerContextWithAppIdTag("am.preserve.serialized_latency")
        ) {
          float progress = ctx.getProgressManager().getProgress();
          checkpoint = vault.generateCheckpoint(progress);
        }

        // Start persisting
        LOG.info("Start preserving checkpoint: {}", checkpoint.toLogString());
        long checkpointSize;
        try (TimerMetric ignored = PrimusMetrics
            .getTimerContextWithAppIdTag("am.preserve.write_latency")
        ) {
          checkpointSize = storage.persistCheckpoint(checkpoint);
        }

        // Emit metrics
        PrimusMetrics.emitStoreWithAppIdTag("am.preserve.size", checkpointSize);
        PrimusMetrics.emitStoreWithAppIdTag("am.preserve.tasks_num",
            checkpoint.getCollectedSerializedTaskNum()
        );
      } catch (IOException e) {
        LOG.warn("Failed to create checkpoint: ", e);
      }

      Sleeper.sleepWithoutInterruptedException(dumpInterval);
    }

    LOG.info("TaskPreserver has stopped");
  }
}
