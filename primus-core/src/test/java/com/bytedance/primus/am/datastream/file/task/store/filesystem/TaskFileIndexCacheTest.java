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
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TaskFileIndexCacheTest extends TaskStoreTestCommon {

  @Test
  public void testTaskFileIndexCache(@TempDir java.nio.file.Path dir) throws IOException {
    // Init
    TaskFileIndexCache cache = new TaskFileIndexCache();
    testSerializeAndDeserialize(cache, dir);

    Assertions.assertFalse(cache.hasTasks());
    Assertions.assertEquals(0, cache.getTaskNum());
    Assertions.assertEquals(-1, cache.getMaxTaskId());
    Assertions.assertEquals(-1, cache.getMaxFileId());

    // Update
    cache.updateIndex(0, 99);
    testSerializeAndDeserialize(cache, dir);

    Assertions.assertTrue(cache.hasTasks());
    Assertions.assertEquals(100, cache.getTaskNum());
    Assertions.assertEquals(99, cache.getMaxTaskId());
    Assertions.assertEquals(0, cache.getMaxFileId());

    Assertions.assertThrows(PrimusRuntimeException.class, () -> cache.getFileId(-1));
    Assertions.assertEquals(0, cache.getFileId(0));
    Assertions.assertEquals(0, cache.getFileId(99));
    Assertions.assertThrows(PrimusRuntimeException.class, () -> cache.getFileId(100));
    Assertions.assertThrows(PrimusRuntimeException.class, () -> cache.getFileId(199));
    Assertions.assertThrows(PrimusRuntimeException.class, () -> cache.getFileId(200));

    // Update again
    cache.updateIndex(1, 199);
    testSerializeAndDeserialize(cache, dir);

    Assertions.assertTrue(cache.hasTasks());
    Assertions.assertEquals(200, cache.getTaskNum());
    Assertions.assertEquals(199, cache.getMaxTaskId());
    Assertions.assertEquals(1, cache.getMaxFileId());

    Assertions.assertThrows(PrimusRuntimeException.class, () -> cache.getFileId(-1));
    Assertions.assertEquals(0, cache.getFileId(0));
    Assertions.assertEquals(0, cache.getFileId(99));
    Assertions.assertEquals(1, cache.getFileId(100));
    Assertions.assertEquals(1, cache.getFileId(199));
    Assertions.assertThrows(PrimusRuntimeException.class, () -> cache.getFileId(200));
  }

  private void testSerializeAndDeserialize(
      TaskFileIndexCache cache,
      java.nio.file.Path dir
  ) throws IOException {
    Path path = new Path(dir.toUri().getPath(), "task-file-index-cache");
    // Serialize
    try (FSDataOutputStream outputStream = fs.create(path)) {
      cache.serialize(outputStream);
    }
    // Deserialize
    try (FSDataInputStream inputStream = fs.open(path)) {
      TaskFileIndexCache restored = TaskFileIndexCache.deserialize(inputStream);
      Assertions.assertEquals(cache, restored);
    }
  }
}
