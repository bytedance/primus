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

package com.bytedance.primus.webapp.bundles;

import static com.bytedance.primus.webapp.StatusServlet.buildTaskUri;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.am.datastream.file.FileTaskManager;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NoArgsConstructor(access = AccessLevel.PUBLIC)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true, access = AccessLevel.PRIVATE)
public class TaskBundle {

  private static final Logger LOG = LoggerFactory.getLogger(TaskBundle.class);

  @Getter
  private long id;
  @Getter
  private String uri;
  @Getter
  private TaskState state;
  @Getter
  private float progress;
  @Getter
  private int numAttempt;
  @Getter
  private Date lastAssignTime;
  @Getter
  private Date finishedTime;
  @Getter
  private String node;
  @Getter
  private String logUrl;
  @Getter
  private String historyLogUrl;

  private TaskBundle(TaskWrapper taskWrapper) {
    TaskStatus taskStatus = taskWrapper.getTaskStatus();
    this.id = taskStatus.getTaskId();
    this.uri = buildTaskUri(taskWrapper.getTask());
    this.state = taskStatus.getTaskState();
    this.progress = taskStatus.getProgress();
    this.numAttempt = taskWrapper.getTask().getNumAttempt();
    this.lastAssignTime = new Date(taskStatus.getLastAssignTime());
    this.finishedTime = new Date(taskStatus.getFinishTime());
    this.node = taskStatus.getAssignedNode();
    this.logUrl = taskStatus.getAssignedNodeUrl();
    this.historyLogUrl = taskStatus.getAssignedNodeUrl();
  }

  private TaskBundle newHistoryBundle() {
    return this.toBuilder()
        .logUrl(this.historyLogUrl) // Overrides logUrl with historyLogUrl
        .build();
  }

  public static List<TaskBundle> newTaskBundles(AMContext context, boolean pruneSucceededTasks) {
    return context
        .getDataStreamManager()
        .getTaskManagerMap()
        .entrySet()
        .stream()
        .flatMap(entry -> {
          String key = entry.getKey();
          TaskManager taskManager = entry.getValue();
          // Ignore irrelevant TaskManagers
          if (!(taskManager instanceof FileTaskManager)) {
            return null;
          }
          // Collect information
          List<TaskBundle> ret = taskManager.getTasksForHistory().stream()
              .map(TaskBundle::new)
              .filter(task -> !pruneSucceededTasks || task.getState() != TaskState.SUCCEEDED)
              .collect(Collectors.toList());
          LOG.info("Find Tasks from:{}, size:{}", key, ret.size());
          return ret.stream();
        })
        .collect(Collectors.toList());
  }

  public static List<TaskBundle> newHistoryBundles(List<TaskBundle> original) {
    return original.stream()
        .map(TaskBundle::newHistoryBundle)
        .collect(Collectors.toList());
  }
}
