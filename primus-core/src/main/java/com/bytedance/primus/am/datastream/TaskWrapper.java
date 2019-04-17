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

package com.bytedance.primus.am.datastream;

import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;


public class TaskWrapper implements Comparable<TaskWrapper> {

  private Task task;
  private TaskStatus taskStatus;

  public TaskWrapper(String group, Task task) {
    this.task = task;
    this.task.setGroup(group);
    taskStatus = new TaskStatusPBImpl();
    taskStatus.setGroup(group);
    taskStatus.setTaskId(task.getTaskId());
    taskStatus.setTaskState(TaskState.RUNNING);
    taskStatus.setProgress(0);
    taskStatus.setCheckpoint(task.getCheckpoint());
    taskStatus.setNumAttempt(task.getNumAttempt());
  }

  public TaskWrapper(String group, Task task, TaskStatus taskStatus) {
    this.task = task;
    this.task.setGroup(group);
    this.taskStatus = taskStatus;
    this.taskStatus.setGroup(group);
  }

  public Task getTask() {
    return task;
  }

  public void setTask(Task task) {
    this.task = task;
  }

  public TaskStatus getTaskStatus() {
    return taskStatus;
  }

  public void setTaskStatus(TaskStatus taskStatus) {
    this.taskStatus = taskStatus;
  }

  @Override
  public int compareTo(TaskWrapper other) {
    assert (this.task.getGroup().equals(other.task.getGroup()));
    if (this.getTask().getTaskId() < other.getTask().getTaskId()) {
      return -1;
    } else if (this.getTask().getTaskId() == other.getTask().getTaskId()) {
      return 0;
    } else {
      return 1;
    }
  }
}
