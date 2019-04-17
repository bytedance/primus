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

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HistoryWebapp extends WebApp {

  private static final Logger LOG = LoggerFactory.getLogger(HistoryWebapp.class);

  private HistoryContext context;

  public HistoryWebapp(HistoryContext context) {
    this.context = context;
  }

  @Override
  public void setup() {
    bind(HsWebServices.class);
    bind(JAXBContextResolver.class);
    bind(GenericExceptionHandler.class);
    bind(HistoryContext.class).toInstance(context);
    route("/", HsController.class);
    route(pajoin("/app", "app.id/"), HsController.class, "app");
  }
}
