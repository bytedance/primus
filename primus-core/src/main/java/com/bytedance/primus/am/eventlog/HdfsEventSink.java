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

package com.bytedance.primus.am.eventlog;

import com.bytedance.primus.am.AMContext;
import java.io.BufferedOutputStream;
import java.io.PrintWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsEventSink implements EventSink {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsEventSink.class);
  private static final int OUTPUT_BUFFER_SIZE = 100 * 1024;
  private static final String IN_PROGRESS = ".inprogress";

  private final FileSystem fs;

  private final Path logPath;
  private final Path workingLogPath;
  private PrintWriter writer;

  public HdfsEventSink(AMContext context) {
    this.fs = context.getHadoopFileSystem();

    String logPathName = getLogPath(context);
    logPath = new Path(logPathName);
    workingLogPath = new Path(logPathName + IN_PROGRESS);
  }

  private String getLogPath(AMContext context) {
    return context.getPrimusConf().getEventLogConfig().getHdfsSink().getDir()
        + "/"
        + context.getAppAttemptId().getApplicationId()
        + "_"
        + context.getAppAttemptId().getAttemptId();
  }

  @Override
  public void init() throws Exception {
    if (fs.exists(workingLogPath)) {
      fs.delete(workingLogPath, true);
    }
    FSDataOutputStream fos = fs.create(workingLogPath);
    BufferedOutputStream bos = new BufferedOutputStream(fos, OUTPUT_BUFFER_SIZE);
    writer = new PrintWriter(bos);
  }

  @Override
  public boolean canLogEvents() {
    return false;
  }

  @Override
  public void stop() throws Exception {
    writer.close();
    if (fs.exists(logPath)) {
      fs.delete(logPath, true);
    }
    fs.rename(workingLogPath, logPath);
  }

  @Override
  public void sink(Object eventJson) throws Exception {
    writer.println(eventJson);
  }
}
