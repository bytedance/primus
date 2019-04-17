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

package com.bytedance.primus.webapp;

import com.bytedance.primus.executor.ExecutorExitCode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryCommandParser {

  private static final Logger LOG = LoggerFactory.getLogger(HistoryCommandParser.class);
  public static final String BASE = "base";
  public static final String PORT = "port";

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
      formatter.printHelp("primus history server help", options);
      System.exit(ExecutorExitCode.CONFIG_PARSE_ERROR.getValue());
    }
    return cmd;
  }

  private static Options generateOption() {
    Options options = new Options();

    Option baseOp = new Option("b", BASE, true, "history hdfs base path");
    baseOp.setRequired(true);
    options.addOption(baseOp);

    Option portOp = new Option("p", PORT, true, "am port");
    portOp.setRequired(true);
    options.addOption(portOp);

    return options;
  }
}
