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
import com.bytedance.primus.common.util.Sleeper;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileBuffer creates a temporary file on the underlying file system as a lock
 */
public class FileLock implements Closeable {

  private static final int RETRY_MAX_TIMES = 100;
  private static final Duration RETRY_SLEEP_DURATION = Duration.ofSeconds(10);

  private final Logger LOG;
  private final FileSystem fs;
  private final Path path;

  public FileLock(FileSystem fs, Path path) {
    this.LOG = LoggerFactory.getLogger(FileLock.class.getName() + "[" + path + "]");
    this.fs = fs;
    this.path = new Path(path + ".lock");

    // Creating a lock on the underlying file system.
    for (int retry = 0; retry < RETRY_MAX_TIMES; ++retry) {
      try {
        fs.create(this.path, false /* overwrite */).close();
        LOG.info("Acquired file lock: {}", path);
        return;

      } catch (IOException e) {
        if (retry % 10 == 0) {
          LOG.warn("Failed to create file lock: path={}, retry={}, err={}", this.path, retry, e);
        }
        Sleeper.sleepWithoutInterruptedException(RETRY_SLEEP_DURATION);
      }
    }

    // Surface the exception
    throw new PrimusRuntimeException(String.format(
        "Failed to create file lock for %s times: path=%s",
        RETRY_MAX_TIMES, path
    ));
  }

  @Override
  public void close() throws IOException {
    fs.delete(path, true /* recursive */);
    LOG.info("Released file lock: {}", path);
  }
}
