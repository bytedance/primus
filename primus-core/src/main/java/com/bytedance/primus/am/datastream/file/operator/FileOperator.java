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

import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy;
import com.bytedance.primus.common.collections.Pair;
import java.util.List;

public interface FileOperator<T extends Input> {

  void setOperatorPolicy(OperatorPolicy operatorPolicy);

  /***
   *
   * @param inputs
   * @return Many partitions and each is a pair, the 1st item of a pair is the partition key
   * @throws Exception
   */
  List<Pair<String, List<T>>> apply(List<T> inputs) throws Exception;

  Pair<String, List<T>> mapPartitions(Pair<String, List<T>> partition);
}
