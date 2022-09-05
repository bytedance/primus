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
import com.bytedance.primus.common.model.records.ContainerId;
import com.bytedance.primus.common.proto.ModelProtos.ApplicationAttemptIdProto;
import com.bytedance.primus.common.proto.ModelProtos.ContainerIdProto;
import com.google.common.base.Preconditions;


public class ContainerIdPBImpl extends ContainerId {

  ContainerIdProto proto = null;
  ContainerIdProto.Builder builder = null;
  private ApplicationAttemptId applicationAttemptId = null;

  public ContainerIdPBImpl() {
    builder = ContainerIdProto.newBuilder();
  }

  public ContainerIdPBImpl(ContainerIdProto proto) {
    this.proto = proto;
    this.applicationAttemptId = convertFromProtoFormat(proto.getAppAttemptId());
  }

  public ContainerIdProto getProto() {
    return proto;
  }

  @Deprecated
  @Override
  public int getId() {
    Preconditions.checkNotNull(proto);
    return (int) proto.getId();
  }

  @Override
  public long getContainerId() {
    Preconditions.checkNotNull(proto);
    return proto.getId();
  }

  @Override
  protected void setContainerId(long id) {
    Preconditions.checkNotNull(builder);
    builder.setId((id));
  }

  @Override
  public int getMigration() {
    Preconditions.checkNotNull(proto);
    return proto.getMigration();
  }

  @Override
  protected void setMigration(int migration) {
    Preconditions.checkNotNull(builder);
    builder.setMigration(migration);
  }


  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  protected void setApplicationAttemptId(ApplicationAttemptId atId) {
    if (atId != null) {
      Preconditions.checkNotNull(builder);
      builder.setAppAttemptId(convertToProtoFormat(atId));
    }
    this.applicationAttemptId = atId;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl) t).getProto();
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}  
