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

package com.bytedance.primus.runtime.yarncommunity.executor;

import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.executor.ContainerImpl;
import com.bytedance.primus.executor.ExecutorCommandParser;
import com.bytedance.primus.executor.ExecutorExitCode;
import com.bytedance.primus.executor.PrimusExecutorConf;
import com.bytedance.primus.executor.exception.PrimusExecutorException;
import com.bytedance.primus.proto.PrimusConfOuterClass;
import com.bytedance.primus.utils.FileUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerMain {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerMain.class);
  public static volatile boolean EXECUTOR_JVM_SHUTDOWN = false;

  public static void main(String[] args) throws Exception {
    ContainerImpl container = new ContainerImpl();
    RunningEnvironmentImpl runningEnvironment = new RunningEnvironmentImpl();
    ShutdownHookManager.get().addShutdownHook(new Thread(() -> {
      LOG.info("Receive shutdown signal.");
      runningEnvironment.setExecutorJvmShutdown(true);
      container.gracefulShutdown();
    }), 1000);

    try {
      LOG.info("init metric collector");
      PrimusMetrics.init(runningEnvironment.getApplicationId() + ".");

      LOG.info("container init...");
      container.init(buildPrimusConf(args), runningEnvironment);

      LOG.info("container start...");
      container.start();
      container.waitForStop();

    } catch (PrimusExecutorException e) {
      LOG.error("runtime error", e);
      System.exit(e.getExitCode());
    }
  }

  public static PrimusExecutorConf buildPrimusConf(String[] args) throws PrimusExecutorException {
    CommandLine cmd = ExecutorCommandParser.getCmd(args);
    String amHost = cmd.getOptionValue(ExecutorCommandParser.AM_HOST);
    int amPort = Integer.parseInt(cmd.getOptionValue(ExecutorCommandParser.AM_PORT));
    String role = cmd.getOptionValue(ExecutorCommandParser.ROLE);
    int index = Integer.parseInt(cmd.getOptionValue(ExecutorCommandParser.INDEX));
    long uniqId = Long.parseLong(cmd.getOptionValue(ExecutorCommandParser.UNIQ_ID));
    String apiServerHost = cmd.getOptionValue(ExecutorCommandParser.APISERVER_HOST);
    int apiServerPort = Integer.parseInt(cmd.getOptionValue(ExecutorCommandParser.APISERVER_PORT));
    String configPath = cmd.getOptionValue(ExecutorCommandParser.CONFIGURATION_PATH);
    String portRangesStr = cmd.getOptionValue(ExecutorCommandParser.PORT_RANGES);

    PrimusConfOuterClass.PrimusConf primusConf;
    try {
      primusConf = FileUtils.buildPrimusConf(configPath);
    } catch (Exception e) {
      throw new PrimusExecutorException(
          "Config parse failed", e, ExecutorExitCode.CONFIG_PARSE_ERROR.getValue());
    }
    return new PrimusExecutorConf(
        amHost,
        amPort,
        role,
        index,
        uniqId,
        apiServerHost,
        apiServerPort,
        primusConf,
        portRangesStr);
  }
}
