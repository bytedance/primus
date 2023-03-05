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

import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CheckpointTest extends TaskStoreTestCommon {

  @Test
  public void testCheckPointFileRotation(@TempDir java.nio.file.Path dir) throws IOException {
    // Init
    Path path = new Path(dir.toUri().getPath(), "checkpoint");

    // Empty
    Assertions.assertTrue(
        Checkpoint.rotateArchiveCheckpoint(
            fs, path,
            0, // checkpointFileSizeMbLimit
            2  // checkpointFileArchiveLimit
        ));
    Assertions.assertEquals(
        0,
        fs.listStatus(path.getParent()).length
    );

    // Rotated 1st time
    fs.createNewFile(new Path(dir.toUri().getPath(), "checkpoint"));
    Assertions.assertTrue(Checkpoint.rotateArchiveCheckpoint(
        fs, path,
        0, // checkpointFileSizeMbLimit
        2  // checkpointFileArchiveLimit
    ));
    Assertions.assertEquals(
        1,
        fs.listStatus(path.getParent()).length
    );

    // Rotated 2nd time
    fs.createNewFile(new Path(dir.toUri().getPath(), "checkpoint"));
    Assertions.assertTrue(Checkpoint.rotateArchiveCheckpoint(
        fs, path,
        0, // checkpointFileSizeMbLimit
        2  // checkpointFileArchiveLimit
    ));
    Assertions.assertEquals(
        2,
        fs.listStatus(path.getParent()).length
    );

    // Rotated 3rd time
    fs.createNewFile(new Path(dir.toUri().getPath(), "checkpoint"));
    Assertions.assertTrue(Checkpoint.rotateArchiveCheckpoint(
        fs, path,
        0, // checkpointFileSizeMbLimit
        2  // checkpointFileArchiveLimit
    ));
    Assertions.assertEquals(
        2,
        fs.listStatus(path.getParent()).length
    );

    // Rotated 4th time
    fs.createNewFile(new Path(dir.toUri().getPath(), "checkpoint"));
    Assertions.assertTrue(Checkpoint.rotateArchiveCheckpoint(
        fs, path,
        0, // checkpointFileSizeMbLimit
        2  // checkpointFileArchiveLimit
    ));
    Assertions.assertEquals(
        2,
        fs.listStatus(path.getParent()).length
    );
  }

  /**
   * testSerializeAndDeserialize tests serializing and deserializing a single checkpoint.
   */
  @Test
  public void testSerializeAndDeserialize(@TempDir java.nio.file.Path dir) throws IOException {
    // Init
    Path path = new Path(dir.toUri().getPath(), "checkpoint");
    Checkpoint original = Checkpoint.newCheckpointToPersist(
        0.5f, // progress,
        2,    // successTaskNum,
        2,    // failureTaskNum,
        9,   // maxRunningTaskId,
        1,    // maxSuccessTaskId,
        3,    // maxFailureTaskId,
        newTaskList(TaskState.RUNNING, 4, 6).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList()),
        newTaskList(TaskState.SUCCEEDED, 0, 2).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList()),
        newTaskList(TaskState.FAILED, 2, 2).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList())
    );

    // Serialize
    try (FSDataOutputStream outputStream = fs.create(path)) {
      original.serializeToOutputStream(outputStream);
    }

    // Deserialize
    Checkpoint restored = Checkpoint.deserialize(fs, path);
    assertCheckpointEquals(original, restored);
  }

  /**
   * testSerializeAndDeserialize tests serializing and deserializing a single checkpoint.
   */
  @Test
  public void testSerializeAndDeserializeWithEdgeConditions(
      @TempDir java.nio.file.Path dir
  ) throws IOException {
    // Init
    Path path = new Path(dir.toUri().getPath(), "checkpoint");
    Checkpoint original = Checkpoint.newCheckpointToPersist(
        0.0f, // progress,
        0,    // successTaskNum,
        0,    // failureTaskNum,
        -1,   // maxRunningTaskId,
        -1,    // maxSuccessTaskId,
        -1,    // maxFailureTaskId,
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>()
    );

    // Serialize
    try (FSDataOutputStream outputStream = fs.create(path)) {
      original.serializeToOutputStream(outputStream);
    }

    // Deserialize
    Checkpoint restored = Checkpoint.deserialize(fs, path);
    assertCheckpointEquals(original, restored);
  }

  /**
   * testSerializeAndDeserialize tests deserializing from a checkpoint file with 2 successfully
   * persisted checkpoints which are followed by a corrupted checkpoint.
   */
  @Test
  public void testCorruptedCheckpointFile(@TempDir java.nio.file.Path dir) throws IOException {
    // Init
    Path path = new Path(dir.toUri().getPath(), "checkpoint");
    Checkpoint original0 = Checkpoint.newCheckpointToPersist(
        0.5f, // progress,
        2,    // successTaskNum,
        2,    // failureTaskNum,
        9,   // maxRunningTaskId,
        1,    // maxSuccessTaskId,
        3,    // maxFailureTaskId,
        newTaskList(TaskState.RUNNING, 4, 6).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList()),
        newTaskList(TaskState.SUCCEEDED, 0, 2).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList()),
        newTaskList(TaskState.FAILED, 2, 2).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList())
    );

    Checkpoint original1 = Checkpoint.newCheckpointToPersist(
        0.6f, // progress,
        2,    // successTaskNum,
        2,    // failureTaskNum,
        11,   // maxRunningTaskId,
        1,    // maxSuccessTaskId,
        3,    // maxFailureTaskId,
        newTaskList(TaskState.RUNNING, 4, 8).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList()),
        newTaskList(TaskState.SUCCEEDED, 0, 2).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList()),
        newTaskList(TaskState.FAILED, 2, 2).stream()
            .map(TaskStatus::toByteArray)
            .collect(Collectors.toList())
    );

    // Serialize
    try (FSDataOutputStream outputStream = fs.create(path)) {
      original0.serializeToOutputStream(outputStream);
      original1.serializeToOutputStream(outputStream);
      outputStream.writeBytes("CORRUPTED-CHECKPOINT");
    }

    // Deserialize
    Checkpoint restored = Checkpoint.deserialize(fs, path);
    assertCheckpointEquals(original1, restored);
  }

  private static List<TaskStatus> newTaskList(TaskState state, int start, int size) {
    return LongStream.range(start, start + size)
        .mapToObj(index -> {
              TaskStatus task = new TaskStatusPBImpl();
              task.setGroup("group");
              task.setTaskId(index);
              task.setSourceId(0);
              task.setTaskState(state);
              task.setProgress(0.5f);
              task.setCheckpoint("checkpoint");
              task.setNumAttempt(1);
              task.setLastAssignTime(System.currentTimeMillis());
              task.setAssignedNode("node");
              task.setAssignedNodeUrl("node-url");
              task.setWorkerName("worker");
              return task;
            }
        ).collect(Collectors.toList());
  }

  private static void assertCheckpointEquals(
      Checkpoint expected,
      Checkpoint got
  ) {
    // Assert statistics
    Assertions.assertEquals(expected.getVersion(), got.getVersion());
    Assertions.assertEquals(expected.getProgress(), got.getProgress());
    Assertions.assertEquals(expected.getSuccessTaskNum(), got.getSuccessTaskNum());
    Assertions.assertEquals(expected.getFailureTaskNum(), got.getFailureTaskNum());
    Assertions.assertEquals(expected.getMaxRunningTaskId(), got.getMaxRunningTaskId());
    Assertions.assertEquals(expected.getMaxSuccessTaskId(), got.getMaxSuccessTaskId());
    Assertions.assertEquals(expected.getMaxFailureTaskId(), got.getMaxFailureTaskId());

    // Assert running tasks
    Assertions.assertEquals(
        expected.serializedRunningTaskStatuses.size(),
        got.getRunningTaskStatuses().size()
    );
    for (int i = 0; i < expected.serializedRunningTaskStatuses.size(); ++i) {
      Assertions.assertArrayEquals(
          expected.serializedRunningTaskStatuses.get(i),
          got.getRunningTaskStatuses().get(i).toByteArray()
      );
    }
  }
}
