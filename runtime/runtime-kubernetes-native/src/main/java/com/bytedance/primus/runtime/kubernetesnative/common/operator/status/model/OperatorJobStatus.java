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

package com.bytedance.primus.runtime.kubernetesnative.common.operator.status.model;

import java.util.Map;

public class OperatorJobStatus {

  private String jobState = "UNDEFINED";

  private Map<String, String> amState;
  private Map<String, String> executorState;
  private long submissionTime;
  private long lastUpdateTime;
  private String message;

  public String getJobState() {
    return jobState;
  }

  public void setJobState(String jobState) {
    this.jobState = jobState;
  }

  public Map<String, String> getAmState() {
    return amState;
  }

  public void setAmState(Map<String, String> amState) {
    this.amState = amState;
  }

  public Map<String, String> getExecutorState() {
    return executorState;
  }

  public void setExecutorState(Map<String, String> executorState) {
    this.executorState = executorState;
  }

  public long getSubmissionTime() {
    return submissionTime;
  }

  public void setSubmissionTime(long submissionTime) {
    this.submissionTime = submissionTime;
  }

  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  public void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
