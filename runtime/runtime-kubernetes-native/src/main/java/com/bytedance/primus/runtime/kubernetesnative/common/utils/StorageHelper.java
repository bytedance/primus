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

package com.bytedance.primus.runtime.kubernetesnative.common.utils;

import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF;
import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_CONF_PATH;

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.utils.ProtoJsonConverter;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This utils is tentatively placed under runtime-kubernetes to segregate the version of the
// dependent Hadoop from primus-core.
public class StorageHelper {

  private static final Logger LOG = LoggerFactory.getLogger(StorageHelper.class);
  private static final FsPermission DEFAULT_FILE_PERMISSION = new FsPermission("777");

  private final FileSystem fileSystem;
  private final Path applicationStagingDir;

  public StorageHelper(FileSystem fileSystem, Path applicationStagingDir) {
    this.fileSystem = fileSystem;
    this.applicationStagingDir = applicationStagingDir;
  }

  public Path getApplicationStagingDir() {
    return applicationStagingDir;
  }

  public void writeConfigFile(PrimusConf primusConf) throws IOException {
    Path configPath = StorageHelper.newJoinedPath(
        fileSystem.getUri().toString(),
        getApplicationStagingDir().toString(),
        PRIMUS_CONF_PATH,
        PRIMUS_CONF
    );

    try (FSDataOutputStream out = fileSystem.create(configPath)) {
      String jsonFormat = ProtoJsonConverter.getJsonStringWithDefaultValueFields(primusConf);
      out.write(jsonFormat.getBytes());
    } catch (IOException e) {
      throw new IOException("Failed to write config", e);
    }
  }

  public void addToLocalResources(Path[] sources) throws IOException {
    addToLocalResources(sources, null);
  }

  public void addToLocalResources(Path[] sources, String subDirName) throws IOException {
    for (Path source : sources) {
      addToLocalResource(source, subDirName);
    }
  }

  public void addToLocalResource(Path src, String subDirName) throws IOException {
    // Compute paths
    Path dst = StorageHelper.newJoinedPath(
        fileSystem.getUri().toString(),
        applicationStagingDir.toString(),
        StringUtils.ensure(subDirName, ""),
        src.getName() // Filename
    );

    // Adding local resource
    if (src.toString().startsWith(fileSystem.getUri().toString())) {
      FileUtil.copy(
          fileSystem, src,
          fileSystem, dst,
          false /* DeleteSource*/, fileSystem.getConf());
    } else {
      LOG.info("Copy local file[{}] to dfs file[{}]", src, dst);
      fileSystem.copyFromLocalFile(false, true, src, dst);
      fileSystem.setPermission(dst, DEFAULT_FILE_PERMISSION);
    }

    LOG.info("Added resource[{}]", dst);
  }

  // TODO: test case
  public static Path[] newPaths(String... paths) {
    return Stream.of(paths)
        .filter(path -> !Strings.isNullOrEmpty(path))
        .map(Path::new)
        .toArray(Path[]::new);
  }

  // TODO: test case
  public static Path newJoinedPath(String... tokens) {
    return newJoinedPath(newPaths(tokens));
  }

  // TODO: test case
  public static Path newJoinedPath(Path... tokens) {
    Optional<Path> path = Stream.of(tokens).reduce(Path::new);
    if (!path.isPresent()) {
      throw new PrimusRuntimeException(
          "Failed to build joined path with [%s]",
          Arrays.toString(tokens));
    }
    return path.get();
  }
}
