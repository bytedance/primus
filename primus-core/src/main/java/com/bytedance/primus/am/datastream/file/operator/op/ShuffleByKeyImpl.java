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

package com.bytedance.primus.am.datastream.file.operator.op;

import com.bytedance.primus.am.datastream.file.operator.Input;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy.OperatorConf;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleByKeyImpl<T extends Input> implements SortByKey<T> {

  private static final Logger log = LoggerFactory.getLogger(ShuffleByKeyImpl.class);

  @Override
  public void setConf(OperatorConf conf) {
  }

  public List<Pair<String, List<T>>> apply(List<Pair<String, List<T>>> inputs) {
    List<String> collect = getInputKeys(inputs);
    log.info("Before ShuffleByKey, {}", collect);
    Collections.shuffle(inputs);
    log.info("After ShuffleByKey, {}", getInputKeys(inputs));
    return inputs;
  }

  public List<String> getInputKeys(List<Pair<String, List<T>>> inputs) {
    return inputs.stream().map(t -> t.getKey()).collect(Collectors.toList());
  }
}
