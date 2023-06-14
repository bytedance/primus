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

package com.bytedance.primus.runtime.yarncommunity.am.container;

import static com.bytedance.primus.apiserver.proto.UtilsProto.YarnScheduler.SchedulerCase.BATCH_SCHEDULER;
import static com.bytedance.primus.apiserver.proto.UtilsProto.YarnScheduler.SchedulerCase.GANG_SCHEDULER;

import com.bytedance.primus.am.role.RoleInfo;
import com.bytedance.primus.am.role.RoleInfoFactory;
import com.bytedance.primus.apiserver.proto.UtilsProto;
import com.bytedance.primus.apiserver.proto.UtilsProto.YarnScheduler.Builder;
import com.bytedance.primus.apiserver.records.RoleSpec;
import com.bytedance.primus.apiserver.utils.ResourceUtils;
import com.bytedance.primus.proto.PrimusRuntime.YarnScheduler;
import com.bytedance.primus.runtime.yarncommunity.am.container.scheduler.fair.FairRoleInfo;

public class YarnRoleInfoFactory implements RoleInfoFactory {

  public YarnRoleInfoFactory() {
  }

  public RoleInfo createRoleInfo(
      Iterable<String> roleNames,
      String roleName,
      RoleSpec roleSpec,
      int priority
  ) {
    return new FairRoleInfo(roleNames, roleName, roleSpec, priority);
  }

  private UtilsProto.YarnScheduler mergeParentSchedulerParam(
      YarnScheduler parent,
      UtilsProto.YarnScheduler child
  ) {
    Builder builder = child.toBuilder();
    if (builder.getResourceTypeValue() == 0) {
      builder.setResourceTypeValue(parent.getResourceTypeValue());
    }
    if (builder.getSchedulingTypeValue() == 0) {
      builder.setSchedulingTypeValue(parent.getSchedulingTypeValue());
    }
    if (builder.getGlobalConstraintTypeValue() == 0) {
      builder.setGlobalConstraintTypeValue(parent.getGlobalConstraintTypeValue());
    }
    if (builder.getNeedGlobalNodesViewValue() == 0) {
      builder.setNeedGlobalNodesViewValue(parent.getNeedGlobalNodesViewValue());
    }
    if (builder.getSchedulerCase() != BATCH_SCHEDULER
        && builder.getSchedulerCase() != GANG_SCHEDULER) {
      builder.setBatchScheduler(ResourceUtils.buildBatchScheduler(parent));
    }
    return builder.build();
  }
}
