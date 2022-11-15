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

import com.bytedance.primus.am.eventlog.PrimusEvent;
import java.util.concurrent.TimeUnit;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ApplicationMasterEvent extends PrimusEvent<ApplicationMasterEventType> {

  private AMContext context;
  private String diagnosis;
  private int exitCode;
  private long gracefulShutdownTimeoutMs;

  public ApplicationMasterEvent(AMContext context, ApplicationMasterEventType type,
      String diagnosis, int exitCode, long gracefulShutdownTimeoutMs) {
    this(context, type, diagnosis, exitCode);
    this.gracefulShutdownTimeoutMs = gracefulShutdownTimeoutMs;
  }

  public ApplicationMasterEvent(AMContext context, ApplicationMasterEventType type,
      String diagnosis, int exitCode) {
    super(type);
    this.context = context;
    this.diagnosis = diagnosis;
    this.exitCode = exitCode;
    long timeoutMs = TimeUnit.MILLISECONDS
        .convert(context.getPrimusConf().getGracefulShutdownTimeoutMin(), TimeUnit.MINUTES);
    this.gracefulShutdownTimeoutMs = timeoutMs;
  }

  public String getDiagnosis() {
    return diagnosis;
  }

  public String getApplicationId() {
    return context.getApplicationId();
  }

  public int getAttemptId() {
    return context.getAttemptId();
  }

  public int getExitCode() {
    return exitCode;
  }

  public long getGracefulShutdownTimeoutMs() {
    return gracefulShutdownTimeoutMs;
  }

  @Override
  public String toJsonString() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("applicationId", getApplicationId());
    json.put("attemptId", getAttemptId());
    json.put("event", ApplicationMasterEvent.class.getCanonicalName());
    json.put("exitCode", getExitCode());
    json.put("timestamp", getTimestamp());
    json.put("eventType", getType());
    json.put("diagnosis", getDiagnosis());
    json.put("getGracefulShutdownTimeoutMs", getGracefulShutdownTimeoutMs());
    return json.toString();
  }
}
