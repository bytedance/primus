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

package com.bytedance.primus.am.progress;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.datastream.DataStreamManager;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.file.FileTaskManager;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileProgressManager extends ProgressManager {

  private static final Logger log = LoggerFactory.getLogger(FileProgressManager.class.getName());

  private final AMContext context;
  private final boolean isRewindAllowed;

  public FileProgressManager(String name, AMContext context) {
    super(name);
    this.context = context;
    isRewindAllowed = isProgressRewindAllowed();
  }

  @Override
  public void update() {
    float totalProgress = 0;
    DataStreamManager dataStreamManager = context.getDataStreamManager();
    int taskManagerNum = 0;
    for (Map.Entry<String, TaskManager> entry : dataStreamManager.getTaskManagerMap().entrySet()) {
      TaskManager taskManager = entry.getValue();
      if (taskManager instanceof FileTaskManager) {
        float progress = taskManager.getProgress();
        PrimusMetrics.emitStoreWithAppIdTag(
            "am.progress_manager.progress",
            new HashMap<String, String>() {{
              put("name", entry.getKey());
            }},
            progress * 100
        );
        if (progress > 0) {
          taskManagerNum += 1;
          totalProgress += progress;
        }
      }
    }
    int totalDatastreamCount = dataStreamManager.getDataSpec().getTotalDatastreamCount();
    if (totalDatastreamCount > 0) {
      taskManagerNum = totalDatastreamCount;
    }
    if (taskManagerNum > 0) {
      totalProgress = totalProgress / taskManagerNum;
      PrimusMetrics.emitStoreWithAppIdTag(
          "am.progress_manager.actually_progress", new HashMap<>(), totalProgress * 100);
      setProgress(isRewindAllowed, totalProgress);
    }
  }

  private boolean isProgressRewindAllowed() {
    if (context.getApplicationMeta().getPrimusConf().getProgressManagerConf().getRewindAllowed()) {
      log.info("Progress rewind allowed.");
      return true;
    }
    return false;
  }
}
