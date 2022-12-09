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

package com.bytedance.primus.io.datasource.file.impl.text;

import com.bytedance.primus.io.datasource.file.FileDataSource;
import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

// TextFileDataSource reads input files line by line, and generates output as key-value pairs, which
// keys are the offsets in the file, while values are the content of each line.
public class TextFileDataSource implements FileDataSource {

  public RecordReader<Object, Object> createRecordReader(
      Configuration hadoopConf,
      FileSplit fileSplit
  ) throws Exception {
    InputFormat<Object, Object> inputFormat =
        (InputFormat<Object, Object>) Class
            .forName(TextInputFormat.class.getCanonicalName())
            .newInstance();
    return inputFormat.getRecordReader(fileSplit, new JobConf(hadoopConf), Reporter.NULL);
  }

  public MessageBuilder createMessageBuilder(int messageBufferSize) {
    return new TextMessageBuilder(messageBufferSize);
  }
}