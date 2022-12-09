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

package com.bytedance.primus.am.datastream.file.operator;

import com.bytedance.primus.am.datastream.file.operator.op.GroupByKey;
import com.bytedance.primus.am.datastream.file.operator.op.Map;
import com.bytedance.primus.am.datastream.file.operator.op.MapPartitions;
import com.bytedance.primus.am.datastream.file.operator.op.OperatorFactory;
import com.bytedance.primus.am.datastream.file.operator.op.SortByKey;
import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy;
import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy.CommonOperatorPolicy;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.io.datasource.file.models.Input;
import java.util.LinkedList;
import java.util.List;

public class CommonFileOperator<T extends Input> implements FileOperator<T> {

  private CommonOperatorPolicy commonOperatorPolicy;

  public CommonFileOperator(CommonOperatorPolicy commonOperatorPolicy) {
    this.commonOperatorPolicy = commonOperatorPolicy;
  }

  @Override
  public void setOperatorPolicy(OperatorPolicy operatorPolicy) {
    this.commonOperatorPolicy = operatorPolicy.getCommonOperatorPolicy();
  }

  @Override
  public List<Pair<String, List<T>>> apply(List<T> inputs) throws Exception {
    List<Pair<String, T>> mapResult = new LinkedList<>();
    Map map = OperatorFactory.getMapOperator(commonOperatorPolicy.getMap());
    for (T t : inputs) {
      mapResult.add(map.apply(t));
    }

    GroupByKey groupByKey = OperatorFactory
        .getGroupByKeyOperator(commonOperatorPolicy.getGroupByKey());
    List<Pair<String, List<T>>> groupByKeyResult = groupByKey.apply(mapResult);

    SortByKey sortByKey = OperatorFactory
        .getSortByKeyOperator(commonOperatorPolicy.getSortByKey());
    return sortByKey.apply(groupByKeyResult);
  }

  @Override
  public Pair<String, List<T>> mapPartitions(Pair<String, List<T>> partition) {
    MapPartitions mapPartitions = OperatorFactory.getMapPartitionsOperator(
        commonOperatorPolicy.getMapPartitionsFunction());
    return mapPartitions.apply(partition);
  }
}
