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

package com.bytedance.primus.am.datastream.env;

import com.bytedance.primus.am.controller.SuspendStatusEnum;
import com.bytedance.primus.am.datastream.TaskManager;
import com.bytedance.primus.am.datastream.TaskManagerState;
import com.bytedance.primus.am.datastream.TaskWrapper;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EnvTaskManager implements TaskManager {

  @Override
  public List<TaskCommand> heartbeat(ExecutorId executorId, List<TaskStatus> taskStatuses,
      boolean removeTask, boolean needMoreTask) {
    return new ArrayList<>();
  }

  @Override
  public void unregister(ExecutorId executorId) {
  }

  @Override
  public List<TaskWrapper> getTasksForHistory() {
    return new ArrayList<>();
  }

  @Override
  public TaskManagerState getState() {
    return TaskManagerState.RUNNING;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public Map<Integer, String> getDataSourceReports() {
    return new HashMap<>();
  }

  @Override
  public boolean isFailure() {
    return false;
  }

  @Override
  public boolean isSuccess() {
    return true;
  }

  @Override
  public void suspend(int snapshotId) {
  }

  @Override
  public SuspendStatusEnum getSuspendStatus() {
    return SuspendStatusEnum.FINISHED_SUCCESS;
  }

  @Override
  public void resume() {
  }

  @Override
  public boolean isSuspend() {
    return false;
  }

  @Override
  public int getSnapshotId() {
    return 0;
  }

  @Override
  public void stop() {
  }

  @Override
  public void succeed() {
  }

  @Override
  public boolean takeSnapshot(String savepointDir) {
    return true;
  }

  @Override
  public DataStreamSpec getDataStreamSpec() {
    return null;
  }

  @Override
  public long getVersion() {
    return 0;
  }
}
