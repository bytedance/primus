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

import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FSDataInputStreamUtilTest {

  @Mock
  FileSystem fileSystem;
  @Mock
  FSDataInputStream fsDataInputStream;

  @Test
  public void testCreateFSDataInputStreamWithRetry() throws IOException {
    Path testPath = new Path("test");
    when(fileSystem.open(testPath))
        .thenThrow(new IOException("N/A"))
        .thenReturn(fsDataInputStream)
        .thenThrow(new IOException("N/A"));
    FSDataInputStreamUtil.createFSDataInputStreamWithRetry(fileSystem, testPath);
    Mockito.verify(fileSystem, Mockito.times(2)).open(testPath);
  }

}
