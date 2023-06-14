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

package com.bytedance.primus.runtime.yarncommunity.client;

import com.bytedance.primus.client.ClientCmdParser;
import com.bytedance.primus.client.ClientCmdRunner;
import com.bytedance.primus.common.util.PrimusConfigurationUtils;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

  private static final Logger LOG = LoggerFactory.getLogger(Client.class);
  private static final String COMMAND_SUBMIT = "submit";

  public static void main(String[] args) throws Exception {
    // Collect command line parameters
    CommandLine commandLine = ClientCmdParser.parse(args);

    String command =
        commandLine.getOptionValue(ClientCmdParser.COMMAND);
    PrimusConf primusConf =
        PrimusConfigurationUtils.load(commandLine.getOptionValue(ClientCmdParser.CONF));
    boolean waitAppCompletion =
        Boolean.parseBoolean(commandLine.getOptionValue(ClientCmdParser.WAIT, "true"));

    // Execute client CMD
    createClientCommandRunner(command, primusConf).run(waitAppCompletion);
  }

  public static ClientCmdRunner createClientCommandRunner(
      String command,
      PrimusConf primusConf
  ) throws Exception {
    switch (command) {
      case COMMAND_SUBMIT:
        LOG.info("start primus submit");
        return new YarnSubmitCmdRunner(primusConf);
      default:
        LOG.info("unsupported command");
        System.exit(1);
    }
    return null;
  }
}
