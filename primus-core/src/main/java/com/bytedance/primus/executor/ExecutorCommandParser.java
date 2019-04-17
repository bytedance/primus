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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorCommandParser {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorCommandParser.class);

  public static final String AM_HOST = "am_host";
  public static final String AM_PORT = "am_port";
  public static final String ROLE = "role";
  public static final String INDEX = "index";
  public static final String UNIQ_ID = "uniq_id";
  public static final String APISERVER_HOST = "apiserver_host";
  public static final String APISERVER_PORT = "apiserver_port";
  public static final String CONFIGURATION_PATH = "conf";
  public static final String PORT_RANGES = "port_ranges";

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
      System.exit(ExecutorExitCode.CONFIG_PARSE_ERROR.getValue());
    }
    return cmd;
  }

  private static Options generateOption() {
    Options options = new Options();

    Option hostOp = new Option("h", AM_HOST, true, "am host");
    hostOp.setRequired(true);
    options.addOption(hostOp);

    Option portOp = new Option("p", AM_PORT, true, "am port");
    portOp.setRequired(true);
    options.addOption(portOp);

    Option roleOp = new Option("r", ROLE, true, "executor role");
    roleOp.setRequired(true);
    options.addOption(roleOp);

    Option indexOp = new Option("i", INDEX, true, "executor index");
    indexOp.setRequired(true);
    options.addOption(indexOp);

    Option uniqIdOp = new Option("u", UNIQ_ID, true, "executor uniq id");
    uniqIdOp.setRequired(true);
    options.addOption(uniqIdOp);

    Option apiServerHostOp = new Option("ah", APISERVER_HOST, true, "api server host");
    hostOp.setRequired(true);
    options.addOption(apiServerHostOp);

    Option apiServerPortOp = new Option("ap", APISERVER_PORT, true, "api server port");
    portOp.setRequired(true);
    options.addOption(apiServerPortOp);

    Option primusConfOp = new Option("c", CONFIGURATION_PATH, true, "primus conf");
    primusConfOp.setRequired(true);
    options.addOption(primusConfOp);

    Option portRangesOp = new Option("pr", PORT_RANGES, true, "port ranges");
    portRangesOp.setRequired(false);
    options.addOption(portRangesOp);

    return options;
  }
}
