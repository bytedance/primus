/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */

package com.bytedance.primus.common.model.records.impl.pb;

import com.bytedance.primus.common.model.records.ApplicationAttemptId;
import com.bytedance.primus.common.model.records.ApplicationId;
import com.bytedance.primus.common.proto.ModelProtos.ApplicationAttemptIdProto;
import com.bytedance.primus.common.proto.ModelProtos.ApplicationIdProto;
import com.google.common.base.Preconditions;


public class ApplicationAttemptIdPBImpl extends ApplicationAttemptId {

  ApplicationAttemptIdProto proto = null;
  ApplicationAttemptIdProto.Builder builder = null;
  private ApplicationId applicationId = null;

  public ApplicationAttemptIdPBImpl() {
    builder = ApplicationAttemptIdProto.newBuilder();
  }

  public ApplicationAttemptIdPBImpl(ApplicationAttemptIdProto proto) {
    this.proto = proto;
    this.applicationId = convertFromProtoFormat(proto.getApplicationId());
  }

  public ApplicationAttemptIdProto getProto() {
    return proto;
  }

  @Override
  public int getAttemptId() {
    Preconditions.checkNotNull(proto);
    return proto.getAttemptId();
  }

  @Override
  protected void setAttemptId(int attemptId) {
    Preconditions.checkNotNull(builder);
    builder.setAttemptId(attemptId);
  }

  @Override
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId appId) {
    if (appId != null) {
      Preconditions.checkNotNull(builder);
      builder.setApplicationId(convertToProtoFormat(appId));
    }
    this.applicationId = appId;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}  
