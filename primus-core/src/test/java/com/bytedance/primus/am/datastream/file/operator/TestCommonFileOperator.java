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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.bytedance.primus.am.datastream.file.PrimusInput;
import com.bytedance.primus.am.datastream.file.PrimusSplit;
import com.bytedance.primus.am.datastream.file.operator.op.MapDelay;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputTypeCase;
import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy.CommonOperatorPolicy;
import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy.OperatorConf;
import com.bytedance.primus.apiserver.proto.DataProto.OperatorPolicy.OperatorType;
import com.bytedance.primus.common.collections.Pair;
import com.bytedance.primus.utils.FileUtils;
import com.bytedance.primus.utils.TimeUtils;
import java.io.File;
import java.text.ParseException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCommonFileOperator {

  private static final Logger LOG = LoggerFactory.getLogger(TestCommonFileOperator.class);

  private static final String sourceA = "sourceA";
  private static final String sourceB = "sourceB";
  private static final String startKey = "2020030223";
  private static final String endKey = "2020030523";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();


  @Test
  public void testMapDelay() throws Exception {
    List<String> args = buildArgumentsForMapDelay();
    List<PrimusInput> rawInputs = buildInputs();
    CommonOperatorPolicy operatorPolicy = buildCommonOperatorPolicyWithMapDelay(args);
    CommonFileOperator fileOperator = new CommonFileOperator(operatorPolicy);
    List<Pair<String, List<PrimusInput>>> pairs = fileOperator.apply(rawInputs);
    List<PrimusInput> inputs = new LinkedList<>();
    for (Pair<String, List<PrimusInput>> pair : pairs) {
      inputs.addAll(pair.getValue());
    }

    String lastKey = "";
    for (PrimusInput input : inputs) {
      assertTrue(input.getKey().compareTo(lastKey) > 0);
      String hourInKey = input.getKey().split(MapDelay.DELIMITER)[0];  // e.g. 2020030223#f1
      String realHourInPath = input.getPath().split("/")[2];  // e.g. /source/time/*
      if (input.getSource().equals(sourceA)) {
        assertEquals(hourInKey, realHourInPath);
      } else {
        assertEquals(input.getSource(), sourceB);
        assertEquals(hourInKey, String.valueOf(
            TimeUtils.plusHour(
                Integer.parseInt(realHourInPath),
                23
            ))
        );
      }
      lastKey = input.getKey();
    }
  }

  @Test
  public void testGetInputTypeForIgnoreFiles() throws java.io.IOException {
    File textFolder = tempFolder.newFolder("text");
    FileSystem fs = FileSystem.get(textFolder.toURI(), new Configuration());
    tempFolder.newFolder("text/train_1_txt_temporary");
    InputTypeCase inputType = FileUtils.getInputType(new Path(textFolder.toURI().getPath()), fs);
    assertEquals(InputTypeCase.RAW_INPUT, inputType);
  }

  private List<PrimusInput> buildInputs() throws ParseException {
    List<PrimusInput> results = new LinkedList<>();
    String hourKey = startKey;
    while (hourKey.compareTo(endKey) <= 0) {
      results.add(
          new PrimusInput(hourKey + "A", sourceA, hourKey, "/" + sourceA + "/" + hourKey + "/*",
              FileSourceSpec.getDefaultInstance()));
      results.add(
          new PrimusInput(hourKey + "B", sourceB, hourKey, "/" + sourceB + "/" + hourKey + "/*",
              FileSourceSpec.getDefaultInstance()));
      hourKey = String.valueOf(TimeUtils.plusHour(Integer.parseInt(hourKey), 1));
    }
    return results;
  }

  private List<String> buildArgumentsForMapDelay() {
    List<String> results = new LinkedList<>();
    results.add(sourceA + MapDelay.DELIMITER + "0" + MapDelay.DELIMITER + "f1");
    results.add(sourceB + MapDelay.DELIMITER + "23" + MapDelay.DELIMITER + "f2");  // delay 23 hours
    return results;
  }

  private CommonOperatorPolicy buildCommonOperatorPolicyWithMapDelay(List<String> args) {
    return CommonOperatorPolicy.newBuilder()
        .setMap(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.MAP_DELAY)
            .addAllArguments(args)
            .build())
        .setGroupByKey(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.GROUP_BY_KEY).build())
        .setMapPartitionsFunction(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.MAP_PARTITIONS_IDENTITY).build())
        .setSortByKey(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.SORT_BY_KEY).build())
        .build();
  }

  @Test
  public void testMapPartitionShuffle() throws Exception {
    List<PrimusSplit> splits = buildSplits();
    CommonOperatorPolicy operatorPolicy = buildCommonOperatorPolicyWithMapPartitionShuffle();
    CommonFileOperator fileOperator = new CommonFileOperator(operatorPolicy);
    List<Pair<String, List<PrimusSplit>>> pairs = fileOperator.apply(splits);
    for (Pair<String, List<PrimusSplit>> pair : pairs) {
      Pair<String, List<PrimusSplit>> shuffle = fileOperator.mapPartitions(pair);
      for (PrimusSplit split : shuffle.getValue()) {
        assertEquals(shuffle.getKey(), split.getKey());
      }
      List<PrimusSplit> tmpSplits = new LinkedList<>(shuffle.getValue());
      Collections.sort(tmpSplits);
      assertNotEquals(shuffle.getValue(), tmpSplits);
    }
  }

  private List<PrimusSplit> buildSplits() throws ParseException {
    List<PrimusSplit> results = new LinkedList<>();
    String hourKey = startKey;
    while (hourKey.compareTo(endKey) <= 0) {
      for (int i = 0; i < 10; i++) {
        results.add(new PrimusSplit(hourKey + "A", sourceA,
            "/" + sourceA + "/" + hourKey + "/file-" + i + "/*", 0, 100,
            hourKey, null));
        results.add(new PrimusSplit(hourKey + "B", sourceB,
            "/" + sourceB + "/" + hourKey + "/file-" + i + "/*", 0, 100,
            hourKey, null));
      }
      hourKey = String.valueOf(TimeUtils.plusHour(Integer.parseInt(hourKey), 1));
    }
    return results;
  }

  private CommonOperatorPolicy buildCommonOperatorPolicyWithMapPartitionShuffle() {
    return CommonOperatorPolicy.newBuilder()
        .setMap(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.MAP_IDENTITY).build())
        .setGroupByKey(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.GROUP_BY_KEY).build())
        .setMapPartitionsFunction(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.MAP_PARTITIONS_SHUFFLE).build())
        .setSortByKey(OperatorConf.newBuilder()
            .setOperatorType(OperatorType.SORT_BY_KEY).build())
        .build();
  }

}
