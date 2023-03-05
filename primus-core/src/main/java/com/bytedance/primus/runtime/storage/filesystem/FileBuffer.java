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

package com.bytedance.primus.runtime.storage.filesystem;

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import java.io.Closeable;
import java.io.IOException;
import lombok.Getter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileBuffer creates a temporary file on the underlying file system and atomically commits the
 * temporary file by renaming it to the designated path on close.
 */
public class FileBuffer implements Closeable {

  private final Logger LOG;
  private final FileSystem fs;
  private final Path targetPath;
  private final Path bufferPath;

  @Getter
  private final FSDataOutputStream bufferedOutputStream;

  public FileBuffer(FileSystem fs, Path path) throws IOException {
    this.LOG = LoggerFactory.getLogger(FileLock.class.getName() + "[" + path + "]");
    this.fs = fs;
    this.targetPath = path;
    this.bufferPath = new Path(path + ".tmp");

    bufferedOutputStream = fs.create(bufferPath, false /* Overwrite */);
  }

  @Override
  public void close() throws IOException {
    if (!fs.exists(bufferPath)) {
      throw new PrimusRuntimeException(
          String.format("Missing BufferFile: path=%s", bufferPath)
      );
    }

    // Close output stream
    bufferedOutputStream.close();

    // Commit the temporary file.
    LOG.info("Committing file({})", bufferPath);
    if (!fs.rename(bufferPath, targetPath)) {
      throw new PrimusRuntimeException(
          String.format(
              "Failed to commit BufferFile: buffer=%s, target=%s",
              bufferPath, targetPath
          )
      );
    }
  }
}
