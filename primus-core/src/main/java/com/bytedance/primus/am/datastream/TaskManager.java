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

import com.bytedance.primus.am.controller.SuspendStatusEnum;
import com.bytedance.primus.api.records.ExecutorId;
import com.bytedance.primus.api.records.TaskCommand;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import java.util.List;
import java.util.Map;

public interface TaskManager {

  List<TaskCommand> heartbeat(ExecutorId executorId, List<TaskStatus> taskStatuses,
      boolean removeTask, boolean needMoreTask);

  void unregister(ExecutorId executorId);

  List<TaskWrapper> getTasksForHistory();

  TaskManagerState getState();

  float getProgress();

  Map<Integer, String> getDataSourceReports();

  boolean isFailure();

  boolean isSuccess();

  void suspend(int snapshotId);

  SuspendStatusEnum getSuspendStatus();

  void resume();

  boolean isSuspend();

  int getSnapshotId();

  void stop();

  void succeed();

  /**
   * Make a savepoint of state to savepointDir.
   *
   * @param savepointDir
   * @return true if succeeded else false
   */
  boolean takeSnapshot(String savepointDir);

  DataStreamSpec getDataStreamSpec();

  long getVersion();
}
