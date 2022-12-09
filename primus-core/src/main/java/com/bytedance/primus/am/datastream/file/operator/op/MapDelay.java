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

import com.bytedance.primus.io.datasource.file.models.Input;
import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy.OperatorConf;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.utils.TimeUtils;
import java.util.HashMap;

public class MapDelay<T extends Input> implements Map<T> {

  public static final String DELIMITER = "#";

  private java.util.Map<String, Integer> sourceDelayMap;
  private java.util.Map<String, String> sourcePriorityMap;

  public MapDelay() {
    sourceDelayMap = new HashMap<>();
    sourcePriorityMap = new HashMap<>();
  }

  /**
   * Arguments of OperatorConf: source_name#delay_number#priority e.g. source1#0#aaa source2#3#bbb
   *
   * @param conf
   */
  @Override
  public void setConf(OperatorConf conf) {
    for (int i = 0; i < conf.getArgumentsCount(); i++) {
      String[] args = conf.getArguments(i).split(DELIMITER);
      sourceDelayMap.put(args[0], Integer.valueOf(args[1]));
      sourcePriorityMap.put(args[0], args[2]);
    }
  }

  @Override
  public Pair<String, T> apply(T input) throws Exception {
    String originKey = input.getKey();
    int delay = sourceDelayMap.get(input.getSource());
    String priority = sourcePriorityMap.get(input.getSource());
    String key;
    if (isDayGranularity(originKey)) {
      key = TimeUtils.plusDay(Integer.parseInt(originKey), delay) + DELIMITER + priority;
    } else {
      key = TimeUtils.plusHour(Integer.parseInt(originKey), delay) + DELIMITER + priority;
    }
    input.setKey(key);
    return new Pair<>(key, input);
  }

  private boolean isDayGranularity(String time) {
    return time.length() == 8;  // YYYYMMDD
  }
}
