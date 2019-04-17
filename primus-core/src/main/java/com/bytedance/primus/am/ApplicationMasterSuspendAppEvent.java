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

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ApplicationMasterSuspendAppEvent extends ApplicationMasterEvent {

  private int snapshotId;

  public ApplicationMasterSuspendAppEvent(AMContext context, String diagnosis) {
    super(context, ApplicationMasterEventType.SUSPEND_APP, diagnosis,
        ApplicationExitCode.SUSPEND.getValue());
    snapshotId = 0;
  }

  public ApplicationMasterSuspendAppEvent(AMContext context, String diagnosis, int snapshotId) {
    super(context, ApplicationMasterEventType.SUSPEND_APP, diagnosis,
        ApplicationExitCode.SUSPEND.getValue());
    this.snapshotId = snapshotId;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  @Override
  public String toJsonString() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("applicationId", getApplicationId());
    json.put("attemptId", getAttemptId());
    json.put("event", ApplicationMasterSuspendAppEvent.class.getCanonicalName());
    json.put("exitCode", getExitCode());
    json.put("timestamp", getTimestamp());
    json.put("eventType", getType());
    json.put("diagnosis", getDiagnosis());
    json.put("snapshotId", String.valueOf(snapshotId));
    return json.toString();
  }
}
