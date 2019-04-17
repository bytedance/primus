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

package com.bytedance.primus.utils;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSDataInputStreamUtil {

  private static final Logger log = LoggerFactory.getLogger(FSDataInputStreamUtil.class);
  private static final int FS_OPEN_DATA_INPUT_STREAM_RETRIES = 2;

  public static FSDataInputStream createFSDataInputStreamWithRetry(FileSystem fs, Path path)
      throws IOException {
    int retries = 0;
    IOException lastException = null;
    while (retries++ < FS_OPEN_DATA_INPUT_STREAM_RETRIES) {
      try {
        return fs.open(path);
      } catch (IOException ex) {
        log.error("Error when create FSDataInputStream, path:" + path + ", retries:" + retries, ex);
        lastException = ex;
      }
    }
    throw new IOException("Open FSDataInputStream failed after retries" + retries, lastException);
  }

}
