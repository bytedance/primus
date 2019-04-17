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

package com.bytedance.primus.runtime.kubernetesnative.am;

import static com.bytedance.primus.runtime.kubernetesnative.common.constants.KubernetesConstants.PRIMUS_K8S_PSM;

import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesJobSpecUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesJobSpecUtil.class);

  private static String extractOwnerNameFromJobName(String jobName) {
    int ownerNameStartIndex = jobName.lastIndexOf("_") + 1;
    String ownerName = jobName.substring(ownerNameStartIndex);
    LOG.info("Extract ownerName:{} from jobName:{}.", ownerName, jobName);
    Preconditions.checkState(ownerNameStartIndex != 0,
        "invalid jobName, try to add ownerName at the end of job_name. current:" + jobName);
    return ownerName;
  }

  public static String getOrComputeOwnerName(PrimusConf primusConf) {
    String ownerNameInPrimusConf = primusConf.getKubernetesJobConf().getOwner();
    if (!Strings.isNullOrEmpty(ownerNameInPrimusConf)) {
      return ownerNameInPrimusConf;
    }
    return extractOwnerNameFromJobName(primusConf.getName());
  }

  public static String getPsmName(PrimusConf primusConf) {
    String psm = primusConf.getKubernetesJobConf().getPsm();
    if (!Strings.isNullOrEmpty(psm)) {
      return psm;
    }
    return PRIMUS_K8S_PSM;
  }
}
