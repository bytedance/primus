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

package com.bytedance.primus.am.progress;

import static com.bytedance.primus.proto.PrimusConfOuterClass.ProgressManagerConf.Type.PM_ROLE;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.exception.PrimusAMException;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.bytedance.primus.proto.PrimusConfOuterClass.ProgressManagerConf.Type;

public class ProgressManagerFactory {

  public static ProgressManager getProgressManager(AMContext context) throws PrimusAMException {
    PrimusConf conf = context.getApplicationMeta().getPrimusConf();
    Type type = conf.hasProgressManagerConf()
        ? conf.getProgressManagerConf().getType()
        : PM_ROLE;

    switch (type) {
      case PM_ROLE:
        return new RoleProgressManager(RoleProgressManager.class.getName(), context);
      case PM_FILE:
        return new FileProgressManager(FileProgressManager.class.getName(), context);
      case PM_KAFKA:
        return new KafkaProgressManager(KafkaProgressManager.class.getName());
      default:
        throw new PrimusAMException("Unsupported progress manager type:" + type.name());
    }
  }
}
