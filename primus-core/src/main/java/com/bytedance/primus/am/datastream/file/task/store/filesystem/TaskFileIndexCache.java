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

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode
@NoArgsConstructor
class TaskFileIndexCache {

  private static final int MIN_FILE_ID = 0;
  private static final long MIN_TASK_ID = 0;

  // Max TaskId -> FileId
  private final TreeMap<Long, Integer> taskIdFileIdMap = new TreeMap<>();

  public static TaskFileIndexCache deserialize(DataInputStream iStream) throws IOException {
    TaskFileIndexCache ret = new TaskFileIndexCache();
    while (iStream.available() > 0) {
      long taskId = iStream.readLong();
      int fileId = iStream.readInt();
      ret.taskIdFileIdMap.put(taskId, fileId);
    }
    return ret;
  }

  public void serialize(DataOutputStream oStream) throws IOException {
    for (Map.Entry<Long, Integer> entry : taskIdFileIdMap.entrySet()) {
      oStream.writeLong(entry.getKey());
      oStream.writeInt(entry.getValue());
    }
  }

  // Mutable methods ===============================================================================
  // ===============================================================================================

  // Returns whether FileTaskIndexCache has been updated.
  public synchronized boolean updateIndex(int fileId, long taskId) {
    if (taskId <= getMaxTaskId()) {
      return false;
    }

    taskIdFileIdMap.put(taskId, fileId);
    return true;
  }

  // Read-only methods =============================================================================
  // ===============================================================================================

  public boolean hasTasks() {
    return !taskIdFileIdMap.isEmpty();
  }

  public long getTaskNum() {
    return getMaxTaskId() - MIN_TASK_ID + 1;
  }

  // Returns -1 if there is no recorded task.
  public int getMaxFileId() {
    return !taskIdFileIdMap.isEmpty()
        ? Collections.max(taskIdFileIdMap.values())
        : MIN_FILE_ID - 1;
  }

  // Returns -1 if there is no recorded task.
  public long getMaxTaskId() {
    return !taskIdFileIdMap.isEmpty()
        ? Collections.max(taskIdFileIdMap.keySet())
        : MIN_TASK_ID - 1;
  }

  public int getFileId(long taskId) throws PrimusRuntimeException {
    if (taskId < MIN_TASK_ID) {
      throw new PrimusRuntimeException("caught invalid taskId(%d)", taskId);
    }

    Entry<Long, Integer> entry = taskIdFileIdMap.ceilingEntry(taskId);
    if (entry == null) {
      throw new PrimusRuntimeException(
          "caught invalid taskId(%d), where max recorded TaskID is %d",
          taskId, getMaxTaskId());
    }
    return entry.getValue();
  }
}
