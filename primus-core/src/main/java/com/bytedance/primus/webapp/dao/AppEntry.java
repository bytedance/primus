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

package com.bytedance.primus.webapp.dao;

import com.bytedance.primus.webapp.bundles.SummaryBundle;
import java.util.Date;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppEntry {

  private String applicationId;
  private String finalStatus;
  private String name;
  private String queue;
  private String user;
  private double progress;
  private Date startTime;
  private String amLogUrl;
  private String jobHistoryUrl;
  private int attemptId;

  public AppEntry() {
  }

  public AppEntry(SummaryBundle bundle) {
    this.applicationId = bundle.getApplicationId();
    this.finalStatus = bundle.getFinalStatus();
    this.name = bundle.getName();
    this.queue = bundle.getQueue();
    this.user = bundle.getUser();
    this.progress = bundle.getProgress();
    this.startTime = bundle.getStartTime();
    this.amLogUrl = bundle.getAmLogUrl();
    this.jobHistoryUrl = bundle.getJobHistoryUrl();
    this.attemptId = bundle.getAttemptId();
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getFinalStatus() {
    return finalStatus;
  }

  public String getName() {
    return name;
  }

  public String getQueue() {
    return queue;
  }

  public String getUser() {
    return user;
  }

  public double getProgress() {
    return progress;
  }

  public Date getStartTime() {
    return startTime;
  }

  public String getAmLogUrl() {
    return amLogUrl;
  }

  public String getJobHistoryUrl() {
    return jobHistoryUrl;
  }

  public int getAttemptId() {
    return attemptId;
  }
}
