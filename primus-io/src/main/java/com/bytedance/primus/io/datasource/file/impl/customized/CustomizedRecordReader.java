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

package com.bytedance.primus.io.datasource.file.impl.customized;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomizedRecordReader
    implements RecordReader<Object /* Text */, Object /* Text */> {

  public static final Logger LOG = LoggerFactory.getLogger(CustomizedRecordReader.class);

  private final String keyPrefix;
  private final String valPrefix;

  private final RecordReader<LongWritable, Text> innerRecordReader;
  private final LongWritable innerKey;
  private final Text innerVal;

  public CustomizedRecordReader(
      FileSplit split,
      Configuration conf,
      String keyPrefix,
      String valPrefix
  ) throws IOException {

    this.keyPrefix = keyPrefix;
    this.valPrefix = valPrefix;

    innerRecordReader = new TextInputFormat()
        .getRecordReader(
            split,
            new JobConf(conf),
            Reporter.NULL
        );

    innerKey = innerRecordReader.createKey();
    innerVal = innerRecordReader.createValue();
  }

  @Override
  public boolean next(Object key, Object val) throws IOException {
    if (!innerRecordReader.next(innerKey, innerVal)) {
      return false;
    }

    ((Text) key).set(String.format("%s-%d", keyPrefix, innerKey.get()).getBytes());
    ((Text) val).set(String.format("%s-%s", valPrefix, innerVal).getBytes());

    return true;
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public long getPos() throws IOException {
    return innerRecordReader.getPos();
  }

  @Override
  public void close() throws IOException {
    innerRecordReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return innerRecordReader.getProgress();
  }
}
