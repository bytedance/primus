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

package com.bytedance.primus.executor.worker.launchplugin;

import com.bytedance.primus.common.child.ChildLaunchPlugin;
import com.bytedance.primus.executor.ExecutorContext;
import com.bytedance.primus.executor.worker.WorkerContext;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvDumpPlugin implements ChildLaunchPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(EnvDumpPlugin.class);

  private ExecutorContext executorContext;
  private WorkerContext workerContext;
  private final String USER_LOGS = "GLOG_log_dir";
  private final String ENV_FILE_NAME = "primus_env";

  public EnvDumpPlugin(ExecutorContext executorContext, WorkerContext workerContext) {
    this.executorContext = executorContext;
    this.workerContext = workerContext;
  }

  @Override
  public void init() throws Exception {
  }

  @Override
  public void preStart() throws Exception {
    try {
      Map<String, String> environment = workerContext.getEnvironment();
      String envFile = Paths.get(environment.get(USER_LOGS), ENV_FILE_NAME).toString();
      try (FileWriter fileWriter = new FileWriter(envFile)) {
        environment.forEach(
            (key, value) -> {
              String line = key + "=" + value + "\n";
              try {
                IOUtils.write(line, fileWriter);
              } catch (IOException e) {
                LOG.error("writing [" + line + "] raise an exception", e);
              }
            });
      }
    } catch (Exception e) {
      LOG.error("Can not dump env ", e);
    }
  }

  @Override
  public void postStart() throws Exception {
  }

  @Override
  public void preStop() throws Exception {
  }

  @Override
  public void postStop() throws Exception {
  }
}
