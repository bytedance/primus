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

import com.esotericsoftware.minlog.Log;
import java.net.InetSocketAddress;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The history server for Primus Applications
 * TODO: Remove hadoop dependencies
 */
public class HistoryServer implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(HistoryServer.class);
  private Configuration conf;

  public static void main(String[] args) throws Exception {
    Log.set(Log.LEVEL_TRACE);
    HistoryServer server = new HistoryServer();
    System.exit(ToolRunner.run(server, args));
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLine cmd = HistoryCommandParser.getCmd(args);
    int port = Integer.parseInt(cmd.getOptionValue(HistoryCommandParser.PORT));
    String hdfsBasePath = cmd.getOptionValue(HistoryCommandParser.BASE);

    HistoryContext context = new HistoryContext(conf, hdfsBasePath);
    HistoryWebapp webApp = new HistoryWebapp(context);
    InetSocketAddress bindAddress = new InetSocketAddress(port);

    LOG.info("Starting HistoryServer");
    WebApps
        .$for(this)
        .with(conf)
        .at(NetUtils.getHostPortString(bindAddress))
        .start(webApp)
        .joinThread();

    return 0;
  }
}
