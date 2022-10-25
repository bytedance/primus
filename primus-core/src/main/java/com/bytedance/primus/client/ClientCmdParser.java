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

package com.bytedance.primus.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientCmdParser {

  private static final Logger LOG = LoggerFactory.getLogger(ClientCmdParser.class);

  // TODO(hopang): move from `_` to `-`.
  public static final String COMMAND = "command";
  public static final String CONF = "primus_conf";
  public static final String WAIT = "wait_app_completion";

  private static final Option[] DEFAULT_CLI_PARAMS = {
      newOption(
          COMMAND,
          "sub-command to execute",
          true /* hasArg */,
          true /* required */),
      newOption(
          CONF,
          "path to primus configuration for the application",
          true /* hasArg */,
          true /* required */),
      newOption(
          WAIT,
          "whether return after sub-command completion, defaults to true",
          true /* hasArg */,
          false /* required */),
  };

  public static CommandLine parse(String[] args, Option... additionalOptions) {
    // get conf
    Options options = generateOption(additionalOptions);
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("primus client help", options);
      System.exit(1);
    }
    return cmd;
  }

  private static Options generateOption(Option... additionalOptions) {
    Options options = new Options();
    for (Option option : DEFAULT_CLI_PARAMS) {
      options.addOption(option);
    }
    for (Option option : additionalOptions) {
      options.addOption(option);
    }
    return options;
  }

  static public Option newOption(
      String name,
      String description,
      boolean hasArg,
      boolean required
  ) {
    Option option = new Option(
        name, // ShortOptionName
        name, // LongOptionName
        hasArg,
        description);
    option.setRequired(required);
    return option;
  }
}
