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

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.PrimusApplicationMeta;
import com.bytedance.primus.api.records.Task;
import com.bytedance.primus.api.records.impl.pb.TaskPBImpl;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusInput.InputManager;
import com.bytedance.primus.proto.PrimusInput.WorkPreserve;
import com.bytedance.primus.proto.PrimusInput.WorkPreserve.HdfsConfig;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class TaskStoreTestCommon {

  protected final FileSystem fs = prepareFileSystem();

  private static FileSystem prepareFileSystem() {
    try {
      return FileSystem.get(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected static AMContext newMockedAMContext() {
    return newMockedAMContext(PrimusConf.getDefaultInstance());
  }

  protected static AMContext newMockedAMContext(Path path) {
    return newMockedAMContext(PrimusConf.getDefaultInstance()
        .toBuilder()
        .setInputManager(
            InputManager.getDefaultInstance()
                .toBuilder()
                .setWorkPreserve(
                    WorkPreserve.getDefaultInstance()
                        .toBuilder()
                        .setHdfsConfig(
                            HdfsConfig.getDefaultInstance()
                                .toBuilder()
                                .setStagingDir(path.toString())
                        )
                )
        )
        .build());
  }

  protected static AMContext newMockedAMContext(PrimusConf primusConf) {
    PrimusApplicationMeta mockedApplicationMeta = Mockito.mock(PrimusApplicationMeta.class);
    Mockito
        .when(mockedApplicationMeta.getApplicationId())
        .thenReturn("mockedApplicationId");
    Mockito
        .when(mockedApplicationMeta.getPrimusConf())
        .thenReturn(primusConf);

    AMContext mockedAMContext = Mockito.mock(AMContext.class);
    Mockito
        .when(mockedAMContext.getApplicationMeta())
        .thenReturn(mockedApplicationMeta);

    return mockedAMContext;
  }

  protected static FileSystemTaskStore newMockedFileSystemTaskStore(AMContext context) {
    FileSystemTaskStore mockedFileSystemTaskStore = Mockito.mock(FileSystemTaskStore.class);
    Mockito
        .when(mockedFileSystemTaskStore.getContext())
        .thenReturn(context);
    Mockito
        .when(mockedFileSystemTaskStore.getName())
        .thenReturn("mocked");

    return mockedFileSystemTaskStore;
  }

  protected static List<Task> newTaskList(int start, int size) {
    return LongStream.range(start, start + size)
        .mapToObj(index -> {
          TaskPBImpl task = new TaskPBImpl();
          task.setGroup("mocked");
          task.setTaskId(index);
          return task;
        }).collect(Collectors.toList());
  }

  protected static void assertTaskListEquals(List<Task> expected, List<Task> got) {
    Assertions.assertEquals(expected.size(), got.size());
    for (int i = 0; i < expected.size(); ++i) {
      TaskPBImpl e = (TaskPBImpl) expected.get(i);
      TaskPBImpl g = (TaskPBImpl) got.get(i);
      Assertions.assertEquals(e.getProto(), g.getProto());
    }
  }

}
