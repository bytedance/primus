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

import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy.OperatorConf;

public class OperatorFactory {

  public static Map getMapOperator(OperatorConf conf) {
    Map operator;
    switch (conf.getOperatorType()) {
      case MAP_DELAY:
        operator = new MapDelay<>();
        break;
      case MAP_IDENTITY:
      default:
        operator = new MapIdentity<>();
        break;
    }
    operator.setConf(conf);
    return operator;
  }

  public static GroupByKey getGroupByKeyOperator(OperatorConf conf) {
    GroupByKey operator;
    switch (conf.getOperatorType()) {
      case GROUP_BY_KEY:
      default:
        operator = new GroupByKeyImpl<>();
        break;
    }
    operator.setConf(conf);
    return operator;
  }

  public static MapPartitions getMapPartitionsOperator(OperatorConf conf) {
    MapPartitions operator;
    switch (conf.getOperatorType()) {
      case MAP_PARTITIONS_SAMPLE:
        operator = new MapPartitionsSample<>();
        break;
      case MAP_PARTITIONS_SHUFFLE:
        operator = new MapPartitionsShuffle<>();
        break;
      case MAP_PARTITIONS_IDENTITY:
      default:
        operator = new MapPartitionsIdentity<>();
        break;
    }
    operator.setConf(conf);
    return operator;
  }

  public static SortByKey getSortByKeyOperator(OperatorConf operatorConf) {
    SortByKey operator;
    switch (operatorConf.getOperatorType()) {
      case SHUFFLE_BY_KEY:
        operator = new ShuffleByKeyImpl<>();
        break;
      case SORT_BY_KEY:
      default:
        operator = new SortByKeyImpl<>();
        break;
    }
    operator.setConf(operatorConf);
    return operator;
  }
}
