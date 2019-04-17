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

import com.bytedance.primus.api.records.InputType;
import com.bytedance.primus.api.records.SplitTask;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.TaskState;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.ExecutorExitCode;
import com.bytedance.primus.executor.exception.PrimusExecutorException;
import com.bytedance.primus.executor.task.WorkerFeeder;
import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import com.bytedance.primus.io.messagebuilder.RawMessageBuilder;
import com.bytedance.primus.io.messagebuilder.TextMessageBuilder;
import com.bytedance.primus.proto.PrimusConfOuterClass.InputManager.MessageBuilderType;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonFileTaskRunner extends FileTaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(CommonFileTaskRunner.class);

  private SplitTask splitTask;
  private InputType inputType;
  private FileSplit fileSplit;
  private RecordReader<Object, Object> recordReader;
  private MessageBuilder messageBuilder;
  private String source;

  public CommonFileTaskRunner(Task task, ExecutorContext context, WorkerFeeder workerFeeder)
      throws IOException {
    super(task, context, workerFeeder);
    splitTask = task.getSplitTask();
    LOG.info("Init task runner, group[" + task.getGroup()
        + ", taskId[" + task.getTaskId() + "]"
        + ", sourceId[" + task.getSourceId() + "]"
        + ", source[" + task.getSource() + "]"
        + ", checkpoint[" + task.getCheckpoint() + "]"
        + ", path[" + splitTask.getPath() + "]"
        + ", start[" + splitTask.getStart() + "]"
        + ", length[" + splitTask.getLength() + "]"
        + ", inputType[" + splitTask.getInputType() + "]");
    source = task.getSource();
    inputType = splitTask.getInputType();
    switch (inputType) {
      case RAW_INPUT:
      case TEXT_INPUT:
        this.fileSplit = new FileSplit(new Path(splitTask.getPath()), splitTask.getStart(),
            splitTask.getLength(), (String[]) null);
        break;
      default:
        throw new IOException("Unsupported input type " + inputType);
    }
  }

  @Override
  public void init() throws Exception {
    try {
      this.recordReader = createRecordReader(jobConf, inputType, source);
      this.messageBuilder = createMessageBuilder(inputType, source);
    } catch (Exception e) {
      this.taskStatus.setTaskState(TaskState.FAILED);
      throw e;
    }
  }

  private RecordReader<Object, Object> createRecordReader(JobConf jobConf, InputType inputType,
      String source) throws Exception {
    InputFormat inputFormat = createInputFormat(jobConf, inputType);
    MessageBuilderType messageBuilderType = context.getPrimusConf()
        .getInputManager().getMessageBuilderType();
    if (messageBuilderType == MessageBuilderType.PLAIN) {
      throw new PrimusExecutorException("Unsupported file input type: " + inputType +
          " in plain message builder type", ExecutorExitCode.CONFIG_PARSE_ERROR.getValue());
    } else {
      return inputFormat.getRecordReader(fileSplit, jobConf, taskReporter);
    }
  }

  public MessageBuilder createMessageBuilder(InputType inputType, String source)
      throws IOException {
    int messageBufferSize = getMessageBufferSize();
    switch (inputType) {
      case TEXT_INPUT:
        return new TextMessageBuilder(messageBufferSize);
      case RAW_INPUT:
        return new RawMessageBuilder(messageBufferSize);
      default:
        throw new IOException("Unsupported file input type: " + inputType);
    }
  }

  @Override
  public RecordReader<Object, Object> getRecordReader() {
    return recordReader;
  }

  @Override
  public RecordReader<Object, Object> createRecordReader() throws Exception {
    recordReader = createRecordReader(jobConf, inputType, source);
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
    if (context.getPrimusConf().getInputManager().getSourceRewindSkipNumCount() != 0 &&
        context.getPrimusConf().getInputManager().getSourceRewindSkipNumMap().containsKey(source)) {
      return context.getPrimusConf().getInputManager().getSourceRewindSkipNumMap().get(source);
    } else {
      return context.getPrimusConf().getInputManager().getWorkPreserve().getRewindSkipNum();
    }
  }
}
