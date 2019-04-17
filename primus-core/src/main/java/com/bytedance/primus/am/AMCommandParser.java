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

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMCommandParser {

  private static final Logger LOG = LoggerFactory.getLogger(AMCommandParser.class);
  public static final String CONFIGURATION_PATH = "config";

  public static CommandLine getCmd(String[] args) {
    // get conf
    Options options = generateOption();
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("primus worker help", options);
      System.exit(ApplicationExitCode.CONFIG_PARSE_ERROR.getValue());
    }
    return cmd;
  }

  private static Options generateOption() {
    Options options = new Options();

    Option configPathOp = new Option("c", "config", true, "primus am configuration.");
    configPathOp.setRequired(true);
    options.addOption(configPathOp);

    return options;
  }
}
