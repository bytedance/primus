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

package com.bytedance.primus.executor;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerNumaBindingCheck {

  public static final String DELAY = "delay";
  private static final Logger LOG = LoggerFactory.getLogger(ContainerNumaBindingCheck.class);
  private static final String USER_LOGS = "GLOG_log_dir";

  private static Options generateCmdOption() {
    Options options = new Options();
    Option delayOp = new Option("de", DELAY, true, "delay time before starting container");
    delayOp.setRequired(true);
    options.addOption(delayOp);
    return options;
  }

  private static CommandLine getCmd(String[] args) {
    // get conf
    Options options = generateCmdOption();
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("primus worker help", options);
      System.exit(ExecutorExitCode.CONFIG_PARSE_ERROR.getValue());
    }
    return cmd;
  }

  public static void main(String[] args) throws Exception {
    try {
      Map<String, String> systemEnvs = System.getenv();
      String logFolderString = systemEnvs.get(USER_LOGS);
      Path path = new Path(logFolderString, "primus_qos");
      CommandLine cmd = getCmd(args);
      long maxDelayInSeconds = Long.parseLong(cmd.getOptionValue("delay"));
      long maxDelay = System.currentTimeMillis() + maxDelayInSeconds * 1000;
      LOG.info("Max number of delay seconds: " + maxDelayInSeconds);
      while (System.currentTimeMillis() <= maxDelay) {
        ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
        File qosFile = new File(path.toUri().toString());
        LOG.info(
            "primus_qos file path " + qosFile.getAbsolutePath() + " existed ? " + qosFile.exists());
        if (qosFile.exists()) {
          DataInputStream dataInputStream = new DataInputStream(
              new FileInputStream(qosFile.getAbsolutePath()));
          String val = IOUtils.toString(dataInputStream);
          dataInputStream.close();
          LOG.info("Primus_qos val: " + val);
          if (val.contains("initialized=true")) {
            LOG.info("Detected primus_qos file");
            break;
          }
        }
        LOG.info("Waiting for primus_qos file ....");
        if (System.currentTimeMillis() > maxDelay) {
          LOG.info("Reach max delay time, will stop waiting here and continue to start container");
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed in main(): {}", e.toString());
    }
  }
}
