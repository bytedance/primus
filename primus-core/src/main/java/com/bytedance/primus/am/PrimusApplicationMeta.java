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

package com.bytedance.primus.am;

import static com.bytedance.primus.utils.PrimusConstants.PRIMUS_VERSION_ENV_KEY;

import com.bytedance.primus.common.network.NetworkConfig;
import com.bytedance.primus.common.util.RuntimeUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusRuntime.PrimusUiConf;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * PrimusApplicationMeta is an immutable object that holds critical information through the entire
 * lifecycle of a Primus application upon its creation.
 */
@Getter
public class PrimusApplicationMeta {

  private final Map<String, String> envs;
  private final String version;

  private final PrimusConf primusConf;
  private final PrimusUiConf primusUiConf;

  private final String username;
  private final String appName;
  private final String applicationId;
  private final int attemptId;
  private final int maxAttemptId;

  // Configurations
  private final Path stagingDir;
  private final int executorTrackerPort;
  private final NetworkConfig networkConfig;

  // Runtime Environment
  private final String nodeId;               // TODO: Create a class to better manage NodeId
  private final FileSystem hadoopFileSystem; // TODO: Create Primus FileSystem interface and abstract direct dependencies on HDFS.

  /**
   * @param primusConf          The comprehensive Primus configurations for the Primus application.
   * @param primusUiConf        The Primus UI configuration for the application.
   * @param nodeId              The nodeId in which Primus Application Master resides.
   * @param applicationId       The applicationId of this Primus application
   * @param attemptId           The attemptId of this Primus application
   * @param username            The name of the user submitted this Primus application
   * @param executorTrackerPort The port used by ExecutorTracker, where valid values are between 0
   *                            and 65535. A port number of zero will let the system pick up an
   *                            ephemeral port in a bind operation.
   * @throws IOException
   */
  public PrimusApplicationMeta(
      PrimusConf primusConf,
      PrimusUiConf primusUiConf,
      String username,
      String applicationId,
      int attemptId,
      int maxAttemptId,
      Path stagingDir,
      int executorTrackerPort,
      String nodeId
  ) throws IOException {

    this.envs = new HashMap<>(System.getenv());
    this.version = envs.get(PRIMUS_VERSION_ENV_KEY);

    this.primusConf = primusConf;
    this.primusUiConf = primusUiConf;

    this.username = username;
    this.appName = primusConf.getName();
    this.applicationId = applicationId;
    this.attemptId = attemptId;
    this.maxAttemptId = maxAttemptId;

    // Configurations
    this.stagingDir = stagingDir;
    this.executorTrackerPort = executorTrackerPort;
    this.networkConfig = new NetworkConfig(primusConf);

    // Runtime Components
    this.nodeId = nodeId;
    this.hadoopFileSystem = RuntimeUtils.loadHadoopFileSystem(primusConf);
  }
}
