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

package com.bytedance.primus.executor.task.file;

import com.bytedance.primus.api.records.SplitTask;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.task.WorkerFeeder;
import com.bytedance.primus.io.datasource.file.FileDataSource;
import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import com.bytedance.primus.proto.PrimusInput.InputManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonFileTaskRunner extends FileTaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(CommonFileTaskRunner.class);

  private final FileDataSource fileDataSource;
  private final FileSplit fileSplit;
  private final String source;

  private RecordReader<Object, Object> recordReader;
  private MessageBuilder messageBuilder;

  public CommonFileTaskRunner(
      Task task,
      ExecutorContext context,
      WorkerFeeder workerFeeder
  ) {
    super(task, context, workerFeeder);

    SplitTask splitTask = task.getSplitTask();
    this.fileDataSource = FileDataSource.load(task.getSplitTask().getSpec());
    this.fileSplit = new FileSplit(
        new Path(splitTask.getPath()),
        splitTask.getStart(),
        splitTask.getLength(),
        (String[]) null
    );
    this.source = task.getSource();

    LOG.info("Init task runner, group[" + task.getGroup()
        + ", taskId[" + task.getTaskId() + "]"
        + ", sourceId[" + task.getSourceId() + "]"
        + ", source[" + task.getSource() + "]"
        + ", checkpoint[" + task.getCheckpoint() + "]"
        + ", path[" + splitTask.getPath() + "]"
        + ", start[" + splitTask.getStart() + "]"
        + ", length[" + splitTask.getLength() + "]"
        + ", spec[" + splitTask.getSpec() + "]");
  }

  @Override
  public void init() throws Exception {
    try {
      this.recordReader = fileDataSource
          .createRecordReader(context.getHadoopFileSystem().getConf(), fileSplit);
      this.messageBuilder = fileDataSource
          .createMessageBuilder(getMessageBufferSize());
    } catch (Exception e) {
      this.taskStatus.setTaskState(TaskState.FAILED);
      throw e;
    }
  }

  @Override
  public RecordReader<Object, Object> getRecordReader() {
    return recordReader;
  }

  @Override
  public RecordReader<Object, Object> createRecordReader() throws Exception {
    this.recordReader = fileDataSource.createRecordReader(
        context.getHadoopFileSystem().getConf(),
        fileSplit
    );
    return recordReader;
  }

  @Override
  public MessageBuilder getMessageBuilder() {
    return messageBuilder;
  }

  @Override
  public long getLength() {
    return fileSplit.getLength();
  }

  @Override
  public int getRewindSkipNum() {
    InputManager inputManager = context
        .getPrimusExecutorConf()
        .getInputManager();

    if (inputManager.getSourceRewindSkipNumCount() != 0 &&
        inputManager.getSourceRewindSkipNumMap().containsKey(source)) {
      return inputManager.getSourceRewindSkipNumMap().get(source);
    }

    return inputManager.getWorkPreserve().getRewindSkipNum();
  }
}
