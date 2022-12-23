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
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompleteApplicationServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(CompleteApplicationServlet.class);
  private static final String TIMEOUT_MS = "timeout_ms";

  @Setter
  private static AMContext context;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    emitApplicationMasterEvent(
        appControllerMethod(req),
        getTimeoutMs(req)
    );

    resp.getOutputStream().print(KILLED_THROUGH_AM_DIAG);
  }

  private long getTimeoutMs(HttpServletRequest req) {
    String timeoutParameter = req.getParameter(TIMEOUT_MS);
    if (!Strings.isNullOrEmpty(timeoutParameter) && StringUtils.isNumeric(timeoutParameter)) {
      return Long.parseLong(timeoutParameter);
    }
    return TimeUnit.MILLISECONDS
        .convert(
            context
                .getApplicationMeta()
                .getPrimusConf()
                .getGracefulShutdownTimeoutMin(),
            TimeUnit.MINUTES);
  }

  private void emitApplicationMasterEvent(String method, long timeoutMs) {
    String dialog = KILLED_THROUGH_AM_DIAG + ", method=" + method + ", timeout(ms):" + timeoutMs;
    switch (method) {
      case "success":
        context.emitApplicationSuccessEvent(
            dialog,
            ApplicationExitCode.SUCCESS_BY_HTTP.getValue(),
            timeoutMs
        );
        break;
      case "fail":
        context.emitFailAttemptEvent(
            dialog,
            ApplicationExitCode.FAIL_ATTEMPT.getValue()
        );
        break;
      case "kill":
        context.emitFailApplicationEvent(
            dialog,
            ApplicationExitCode.FAIL_APP.getValue()
        );
        break;
      default:
        LOG.warn("Received unknown Primus Web API: {}", method);
    }
  }

  private String appControllerMethod(HttpServletRequest request) {
    String[] split = request.getServletPath().split("/");
    return split[split.length - 1];
  }
}


