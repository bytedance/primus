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

package com.bytedance.primus.io.datasource.file;

import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.CustomizedInput;
import com.bytedance.primus.io.datasource.file.impl.raw.RawFileDataSource;
import com.bytedance.primus.io.datasource.file.impl.text.TextFileDataSource;
import com.bytedance.primus.io.datasource.file.models.PrimusInput;
import com.bytedance.primus.io.datasource.file.models.PrimusSplit;
import com.bytedance.primus.io.messagebuilder.MessageBuilder;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

// TODO: Comment
public interface FileDataSource {

  RecordReader<Object, Object> createRecordReader(
      Configuration hadoopConf,
      FileSplit fileSplit
  ) throws Exception;

  MessageBuilder createMessageBuilder(int messageBufferSize);

  static FileDataSource load(FileSourceSpec spec) {
    switch (spec.getInputTypeCase()) {
      case RAW_INPUT:
        return new RawFileDataSource();
      case TEXT_INPUT:
        return new TextFileDataSource();
      case CUSTOMIZED_INPUT:
        CustomizedInput input = spec.getCustomizedInput();
        try {
          return (FileDataSource) Class
              .forName(input.getClassName())
              .getConstructor(Map.class)
              .newInstance(input.getParamsMap());
        } catch (Exception e) {
          throw new RuntimeException(String.format(
              "Failed to initiate FileDataSource for Type(%s): %s",
              input.getClassName(), e)
          );
        }
      default:
        throw new IllegalArgumentException("Missing or unknown InputType: " + spec);
    }
  }

  default SortedSet<PrimusSplit> scanPattern(
      FileSystem fs,
      PrimusInput input
  ) throws IllegalArgumentException, IOException {
    // Glob file system
    FileStatus[] matches = fs.globStatus(new Path(input.getPath()));
    if (matches == null) {
      throw new NoSuchFileException("Input path does not exist: " + input.getPath());
    } else if (matches.length == 0) {
      throw new NoSuchFileException("Input Pattern " + input.getPath() + " matches 0 files");
    }

    // Assemble results
    SortedSet<PrimusSplit> ret = new TreeSet<>();
    for (FileStatus globStat : matches) {
      FileStatus[] fileStatuses = globStat.isDirectory()
          ? fs.listStatus(new Path(globStat.getPath().toUri().getPath()))
          : new FileStatus[]{globStat};

      ret.addAll(Arrays.stream(fileStatuses)
          .filter(fileStatus -> !isIgnoredFile(fileStatus.getPath()))
          .map(fileStatus -> new PrimusSplit(
              input,
              fileStatus.getPath().toString(),
              0 /* start */,
              fileStatus.getLen()
          ))
          .collect(Collectors.toList())
      );
    }

    return ret;
  }

  default boolean isIgnoredFile(Path path) {
    return path.getName().endsWith("_SUCCESS")
        || path.getName().equals("_temporary")
        || path.toUri().getPath().contains("/_temporary/");
  }
}
