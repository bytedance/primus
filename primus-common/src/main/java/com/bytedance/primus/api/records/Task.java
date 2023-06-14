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

package com.bytedance.primus.api.records;

import org.apache.hadoop.io.Writable;

public interface Task extends Writable {

  String getGroup();

  void setGroup(String group);

  long getTaskId();

  void setTaskId(long taskId);

  int getSourceId();

  void setSourceId(int sourceId);

  String getSource();

  void setSource(String source);

  String getCheckpoint();

  void setCheckpoint(String checkpoint);

  TaskType getTaskType();

  FileTask getFileTask();

  void setFileTask(FileTask fileTask);

  KafkaTask getKafkaTask();

  void setKafkaTask(KafkaTask kafkaTask);

  int getNumAttempt();

  void setNumAttempt(int numAttempt);

  String getUid();
}
