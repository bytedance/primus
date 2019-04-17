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

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationMasterSuspendAppEvent;
import com.bytedance.primus.am.datastream.DataStreamManagerEvent;
import com.bytedance.primus.am.datastream.DataStreamManagerEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class SuccessDataStreamServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(SuccessDataStreamServlet.class);
  private static AMContext context;

  public static void setContext(AMContext context) {
    SuccessDataStreamServlet.context = context;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String diag = "DataStream succeed by someone through http request";
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        LOG.warn(diag);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          // ignore
        }
        LOG.warn("sending ApplicationMasterEvent.SUSPEND_APP");
        context.getDispatcher().getEventHandler().handle(
            new DataStreamManagerEvent(
                DataStreamManagerEventType.DATA_STREAM_SUCCEED,
                context.getDataStreamManager().getDataSpec(),
                0L)); // Set version to 0, cause event handler do not need version number
      }
    });
    thread.setName("SucceedDataStreamHTTPRespondThread");
    thread.setDaemon(true);
    thread.start();
    resp.getOutputStream().print(diag);
  }
}
