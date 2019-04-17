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

import static com.bytedance.primus.utils.PrimusConstants.KILLED_THROUGH_AM_DIAG;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompleteApplicationServlet extends HttpServlet {
  private static final Logger log = LoggerFactory.getLogger(CompleteApplicationServlet.class);
  private static final String TIMEOUT_MS = "timeout_ms";

  private static AMContext context;
  private static final Map<String, Pair<ApplicationMasterEventType, ApplicationExitCode>> action_dict = ImmutableMap.of(
      "fail", Pair.of(ApplicationMasterEventType.FAIL_ATTEMPT, ApplicationExitCode.FAIL_ATTEMPT_BY_HTTP),
      "kill", Pair.of(ApplicationMasterEventType.FAIL_APP, ApplicationExitCode.KILLED_BY_HTTP),
      "success", Pair.of(ApplicationMasterEventType.SUCCESS, ApplicationExitCode.SUCCESS_BY_HTTP)
  );

  public static void setContext(AMContext context) {
    CompleteApplicationServlet.context = context;
  }


  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    long timeoutMs = 0;
    String timeoutParameter = req.getParameter(TIMEOUT_MS);
    if (!Strings.isNullOrEmpty(timeoutParameter) && StringUtils.isNumeric(timeoutParameter)) {
      timeoutMs = Long.parseLong(timeoutParameter);
    } else {
      timeoutMs = TimeUnit.MILLISECONDS
          .convert(context.getPrimusConf().getGracefulShutdownTimeoutMin(), TimeUnit.MINUTES);
    }

    String method = appControllerMethod(req);
    ApplicationMasterEvent applicationMasterEvent = getApplicationMasterEvent(method, timeoutMs);
    context.getDispatcher().getEventHandler().handle(applicationMasterEvent);
    resp.getOutputStream().print(KILLED_THROUGH_AM_DIAG);
  }


  private ApplicationMasterEvent getApplicationMasterEvent(String method, long timeoutMs) {
    String dialog = KILLED_THROUGH_AM_DIAG + ", method=" + method + ", timeout(ms):" + timeoutMs;
    ApplicationMasterEventType eventType = action_dict.get(method).getKey();
    ApplicationExitCode exitCode = action_dict.get(method).getValue();
    ApplicationMasterEvent applicationMasterEvent = new ApplicationMasterEvent(context, eventType,
        dialog,
        exitCode.getValue(), timeoutMs);
    try{
      log.info("getApplicationMasterEvent:{}", applicationMasterEvent.toJsonString());
    }catch (Exception ex){
      log.error("error when print ApplicationMasterEvent.", applicationMasterEvent);
    }
    return applicationMasterEvent;
  }

  private String appControllerMethod(HttpServletRequest request) {
    String[] split = request.getServletPath().split("/");
    return split[split.length - 1];
  }
}


