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

import com.bytedance.primus.api.records.TaskStatus;
import com.bytedance.primus.api.records.impl.pb.TaskStatusPBImpl;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusCommon.Version;
import com.bytedance.primus.runtime.storage.filesystem.PrimusStateStorage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checkpoint allows Primus applications to resume from their previous executions by preserving the
 * status and statistics of both running and completed tasks.
 */
// TODO: Create an interface when needed.
public class Checkpoint {

  private static final String END_MARKER = "END";
  private static final int CHKPT_FILE_ARCHIVE_LIMIT = 10;
  private static final int CHKPT_FILE_SIZE_MB_LIMIT = 10 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(Checkpoint.class);

  private enum Origin {
    TASK_STORAGE, // loaded from storage
    TASK_VAULT,   // to save to storage
  }

  private final Origin origin;

  @Getter
  private final Version version;
  @Getter
  private final float progress;
  @Getter
  private final long successTaskNum;
  @Getter
  private final long failureTaskNum;
  @Getter
  private final long maxRunningTaskId;
  @Getter
  private final long maxSuccessTaskId;
  @Getter
  private final long maxFailureTaskId;

  // For Origin.TASK_STORAGE
  private final List<TaskStatus> runningTaskStatuses;

  // For Origin.TASK_VAULT
  final List<byte[]> serializedRunningTaskStatuses;
  final List<byte[]> serializedSuccessTaskStatuses;
  final List<byte[]> serializedFailureTaskStatuses;

  // Created for fresh starts when there is no existing checkpoint.
  public Checkpoint() {
    this.origin = Origin.TASK_STORAGE;
    this.version = Version.V2; // Only V2 is supported for now.

    progress = 0;
    successTaskNum = 0;
    failureTaskNum = 0;
    maxRunningTaskId = -1;
    maxSuccessTaskId = -1;
    maxFailureTaskId = -1;

    runningTaskStatuses = new LinkedList<>();
    serializedRunningTaskStatuses = null; // Ignored
    serializedSuccessTaskStatuses = null; // Ignored
    serializedFailureTaskStatuses = null; // Ignored
  }

  // Created from an existing checkpoint file.
  private Checkpoint(
      Version version,
      float progress,
      long successTaskNum,
      long failureTaskNum,
      long maxRunningTaskId,
      long maxSuccessTaskId,
      long maxFailureTaskId,
      List<TaskStatus> runningTaskStatuses
  ) {
    this.origin = Origin.TASK_STORAGE;
    this.version = version;

    this.progress = progress;
    this.successTaskNum = successTaskNum;
    this.failureTaskNum = failureTaskNum;
    this.maxRunningTaskId = maxRunningTaskId;
    this.maxSuccessTaskId = maxSuccessTaskId;
    this.maxFailureTaskId = maxFailureTaskId;

    this.runningTaskStatuses = runningTaskStatuses;
    this.serializedRunningTaskStatuses = null; // Ignored
    this.serializedSuccessTaskStatuses = null; // Ignored
    this.serializedFailureTaskStatuses = null; // Ignored
  }

  // Created to be persisted on storage
  private Checkpoint(
      Version version,
      float progress,
      long successTaskNum,
      long failureTaskNum,
      long maxRunningTaskId,
      long maxSuccessTaskId,
      long maxFailureTaskId,
      List<byte[]> serializedRunningTaskStatuses,
      List<byte[]> serializedSuccessTaskStatuses,
      List<byte[]> serializedFailureTaskStatuses
  ) {
    this.origin = Origin.TASK_VAULT;
    this.version = version;

    this.progress = progress;
    this.successTaskNum = successTaskNum;
    this.failureTaskNum = failureTaskNum;
    this.maxRunningTaskId = maxRunningTaskId;
    this.maxSuccessTaskId = maxSuccessTaskId;
    this.maxFailureTaskId = maxFailureTaskId;

    this.runningTaskStatuses = new LinkedList<>();
    this.serializedRunningTaskStatuses = serializedRunningTaskStatuses;
    this.serializedSuccessTaskStatuses = serializedSuccessTaskStatuses;
    this.serializedFailureTaskStatuses = serializedFailureTaskStatuses;
  }

  public static Checkpoint newCheckpointToPersist(
      float progress,
      long successTaskNum,
      long failureTaskNum,
      long maxRunningTaskId,
      long maxSuccessTaskId,
      long maxFailureTaskId,
      List<byte[]> serializedRunningTaskStatuses,
      List<byte[]> serializedSuccessTaskStatuses,
      List<byte[]> serializedFailureTaskStatuses
  ) {
    return new Checkpoint(
        Version.V2, // Only V2 is supported for now
        progress,
        successTaskNum,
        failureTaskNum,
        maxRunningTaskId,
        maxSuccessTaskId,
        maxFailureTaskId,
        serializedRunningTaskStatuses,
        serializedSuccessTaskStatuses,
        serializedFailureTaskStatuses
    );
  }

  /**
   * Reconstructs a checkpoint from the specify file or its corresponding archives when necessary.
   *
   * @param fs   the file system
   * @param path the path of the checkpoint file
   * @return a reconstructed checkpoint
   */
  public static Checkpoint deserialize(FileSystem fs, Path path) throws IOException {
    // Curate candidate checkpoint files
    List<Path> candidateCheckpointFiles = new LinkedList<Path>() {{
      add(path);
      addAll(getArchivedCheckpointFileDescendingList(fs, path));
    }};
    // Start loading from the most recent checkpoint file
    for (Path candidate : candidateCheckpointFiles) {
      Checkpoint checkpoint = deserializeFromFile(fs, candidate);
      if (checkpoint != null) {
        return checkpoint;
      }
    }
    // Fallback to default checkpoint for fresh start.
    LOG.info("A fresh checkpoint is created");
    return new Checkpoint();
  }

  // load the latest healthy checkpoint from a single file.
  private static Checkpoint deserializeFromFile(FileSystem fs, Path path) {
    try (FSDataInputStream iStream = PrimusStateStorage.open(fs, path)) {
      // Try directly loading the latest checkpoint
      try {
        LOG.info("Start deserializing checkpoint from {}", path);
        iStream.seek(fs.getFileStatus(path).getLen() - Long.BYTES /* position pointer size */);
        iStream.seek(iStream.readLong());
        Checkpoint checkpoint = Checkpoint.deserializeFromIStream(iStream);
        if (checkpoint != null) {
          return checkpoint;
        }
      } catch (IOException e) {
        LOG.warn("Failed to load the latest checkpoint from path={}, err={}", path, e);
      }
      // Fallback to iterating from the beginning of the checkpoint file
      try {
        LOG.warn("Failed to deserialize the latest checkpoint, try iterating from start instead");
        iStream.seek(0);
        Checkpoint prev = null;
        Checkpoint curr = null;
        while (true) {
          prev = curr;
          curr = Checkpoint.deserializeFromIStream(iStream);
          if (curr == null) {
            return prev;
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed to load a checkpoint from path={}, err={}", path, e);
      }
    } catch (IOException e) {
      LOG.warn("Failed to open checkpoint file from path={}, err={}", path, e);
    }
    return null;
  }

  // Deserialize a checkpoint from the given iStream
  private static Checkpoint deserializeFromIStream(FSDataInputStream iStream) {
    try {
      long start = iStream.getPos();

      // Collect header
      byte[] versionBuffer = new byte[iStream.readInt()];
      iStream.readFully(versionBuffer);
      long timestamp = iStream.readLong();

      // Collect meta
      float progress = iStream.readFloat();
      long successTaskNum = iStream.readLong();
      long failureTaskNum = iStream.readLong();
      long maxSuccessTaskId = iStream.readLong();
      long maxFailureTaskId = iStream.readLong();

      // Collect tasks
      int runningTaskNum = iStream.readInt();
      List<TaskStatus> runningTaskStatuses = new ArrayList<>(runningTaskNum);
      for (int i = 0; i < runningTaskNum; ++i) {
        TaskStatus taskStatus = new TaskStatusPBImpl();
        taskStatus.readFields(iStream);
        runningTaskStatuses.add(taskStatus);
      }

      // Check footer
      byte[] endMarkerBuffer = new byte[END_MARKER.length()];
      iStream.readFully(endMarkerBuffer);
      if (!END_MARKER.equals(new String(endMarkerBuffer)) || start != iStream.readLong()) {
        LOG.warn("Failed to load checkpoint from a corrupted checkpoint file");
        return null;
      }

      // Compute maxRunningTaskId
      long maxRunningTaskId = runningTaskStatuses.stream()
          .map(TaskStatus::getTaskId)
          .reduce(-1L, Math::max);

      // Successfully loaded a checkpoint from stream.
      Checkpoint checkpoint = new Checkpoint(
          Version.valueOf(new String(versionBuffer)),
          progress,
          successTaskNum,
          failureTaskNum,
          maxRunningTaskId,
          maxSuccessTaskId,
          maxFailureTaskId,
          runningTaskStatuses
      );
      LOG.info("Loaded a checkpoint created at {}: {}", timestamp, checkpoint.toLogString());
      return checkpoint;

    } catch (IOException e) {
      LOG.warn("Failed to load checkpoint from a corrupted checkpoint file");
      return null;
    }
  }

  /**
   * preserveRunningTaskStatus() persists running tasks to the designated file system on the given
   * path. This method renames the existing file to <NAME>.archived.<TIMESTAMP> when needed, and
   * then creates a new file on the path.
   *
   * @param fs   the file system to persist
   * @param path the path to persist the checkpoint file
   * @return the number of bytes written to the oStream.
   */
  public long preserveRunningTaskAndStatistics(FileSystem fs, Path path) throws IOException {
    // Rotate checkpoint file if needed
    boolean rotated = rotateArchiveCheckpoint(
        fs, path,
        CHKPT_FILE_SIZE_MB_LIMIT,
        CHKPT_FILE_ARCHIVE_LIMIT
    );

    // Persist checkpoint onto the assigned path
    try (FSDataOutputStream oStream = rotated
        ? fs.create(path, false /* override*/)
        : fs.append(path)
    ) {
      return this.serializeToOutputStream(oStream);
    }
  }

  static boolean rotateArchiveCheckpoint(
      FileSystem fs,
      Path path,
      int checkpointFileSizeMbLimit,
      int checkpointFileArchiveLimit
  ) throws IOException {
    // No existing checkpoint file
    if (!fs.exists(path)) {
      return true;
    }
    // Can still append to the checkpoint file
    if (fs.exists(path) && fs.getFileStatus(path).getLen() < checkpointFileSizeMbLimit) {
      return false;
    }
    // Delete stale checkpoint files
    List<Path> archivedFiles = getArchivedCheckpointFileDescendingList(fs, path);
    for (int i = Math.max(0, checkpointFileArchiveLimit - 1); i < archivedFiles.size(); ++i) {
      fs.delete(archivedFiles.get(i), false);
    }
    // Create a new archive checkpoint file
    fs.rename(path, new Path(path + ".archived." + System.currentTimeMillis()));
    return true;
  }

  long serializeToOutputStream(FSDataOutputStream oStream) throws IOException {
    // Capture the current offset
    long start = oStream.getPos();

    // Write header
    oStream.writeInt(version.name().length());
    oStream.writeBytes(version.name());
    oStream.writeLong(System.currentTimeMillis() / 1000);

    // write meta
    oStream.writeFloat(progress);
    oStream.writeLong(successTaskNum);
    oStream.writeLong(failureTaskNum);
    oStream.writeLong(maxSuccessTaskId);
    oStream.writeLong(maxFailureTaskId);

    // write running task status
    oStream.writeInt(serializedRunningTaskStatuses.size());
    for (byte[] status : serializedRunningTaskStatuses) {
      oStream.writeInt(status.length);
      oStream.write(status);
    }

    // Write footer
    oStream.writeBytes(END_MARKER);
    oStream.writeLong(start);

    return oStream.size() - start;
  }

  private static List<Path> getArchivedCheckpointFileDescendingList(
      FileSystem fs,
      Path path
  ) throws IOException {
    String prefix = path.getName() + ".archived.";
    return Arrays.stream(fs.listStatus(path.getParent()))
        .map(FileStatus::getPath)
        .filter(p -> p.getName().startsWith(prefix))
        .sorted((a, b) -> -1 * StringUtils.compareTimeStampStrings(
            a.getName().substring(prefix.length()),
            b.getName().substring(prefix.length())
        ))
        .collect(Collectors.toList());
  }

  // Return the number of bytes written to the oStream.
  public long preserveSuccessTasks(FileSystem fs, Path path) throws IOException {
    return preserveFinishTasks(fs, path, serializedSuccessTaskStatuses);
  }

  // Return the number of bytes written to the oStream.
  public long preserveFailureTasks(FileSystem fs, Path path) throws IOException {
    return preserveFinishTasks(fs, path, serializedFailureTaskStatuses);
  }

  // Return the number of bytes written to the oStream.
  private long preserveFinishTasks(
      FileSystem fs,
      Path path,
      List<byte[]> taskStatuses
  ) throws IOException {
    if (taskStatuses == null ||
        taskStatuses.isEmpty()) {
      return 0;
    }

    try (FSDataOutputStream oStream = fs.exists(path)
        ? fs.append(path)
        : fs.create(path)
    ) {
      long size = oStream.size();
      for (byte[] status : taskStatuses) {
        oStream.writeInt(status.length);
        oStream.write(status);
      }
      return oStream.size() - size;
    }
  }

  public List<TaskStatus> getRunningTaskStatuses() {
    if (origin == Origin.TASK_VAULT) {
      throw new UnsupportedOperationException(
          "getRunningTaskStatuses doesn't support Origin.TASK_VAULT");
    }
    return runningTaskStatuses;
  }

  public long getCollectedSerializedTaskNum() {
    return (serializedRunningTaskStatuses == null ? 0L : serializedRunningTaskStatuses.size())
        + (serializedSuccessTaskStatuses == null ? 0L : serializedSuccessTaskStatuses.size())
        + (serializedFailureTaskStatuses == null ? 0L : serializedFailureTaskStatuses.size());
  }

  public String toLogString() {
    return "successTaskNum: " + successTaskNum
        + ", maxSuccessTaskId: " + maxSuccessTaskId
        + ", failureTaskNum: " + failureTaskNum
        + ", maxFailureTaskId: " + maxFailureTaskId
        + ", runningTaskNum: " +
        (runningTaskStatuses != null
            ? runningTaskStatuses.size()
            : serializedRunningTaskStatuses != null
                ? serializedRunningTaskStatuses.size()
                : 0);
  }
}
