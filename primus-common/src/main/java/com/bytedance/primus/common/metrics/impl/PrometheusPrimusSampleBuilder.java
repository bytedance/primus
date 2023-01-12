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

package com.bytedance.primus.common.metrics.impl;

import com.bytedance.primus.common.collections.Pair;
import io.prometheus.client.Collector;
import io.prometheus.client.dropwizard.samplebuilder.SampleBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusPrimusSampleBuilder implements SampleBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(PrometheusPrimusSampleBuilder.class);

  @Override
  public Collector.MetricFamilySamples.Sample createSample(
      final String dropwizardName,
      final String nameSuffix,
      final List<String> additionalLabelNames,
      final List<String> additionalLabelValues,
      final double value
  ) {
    // Restore information
    MetricId metricId = new MetricId(dropwizardName);

    // Restore labels
    final List<String> labelNames = additionalLabelNames == null
        ? new LinkedList<>()
        : additionalLabelNames;
    final List<String> labelValues = additionalLabelValues == null
        ? new LinkedList<>()
        : additionalLabelValues;

    metricId.labels.forEach((key, val) -> {
      labelNames.add(key);
      labelValues.add(val);
    });

    // Assemble result
    return new Collector.MetricFamilySamples.Sample(
        metricId.name + nameSuffix,
        labelNames,
        labelValues,
        value
    );
  }

  // TODO: Find a friendly MetricRegistry for PrimusMetrics to prevent manual parsing labels here.
  public static class MetricId {

    public final String name;
    public final Map<String, String> labels;

    MetricId(String input) {
      int sIdx = input.indexOf("{");
      int eIdx = input.indexOf("}");
      if (sIdx == -1 || eIdx == -1 || sIdx >= eIdx) {
        this.name = input;
        this.labels = new HashMap<>();
        return;
      }

      this.name = input.substring(0, sIdx);
      this.labels = Arrays
          .stream(input
              .substring(sIdx + 1, eIdx)
              .split(",")
          )
          .map(token -> new Pair<>(token, token.indexOf("=")))
          .filter(pair -> {
            if (pair.getValue() <= 0) {
              LOG.warn("Caught malformed metric name: {}", pair.getKey());
              return false;
            }
            return true;
          })
          .map(pair -> {
            String token = pair.getKey();
            int mIdx = pair.getValue();
            return new Pair<>(
                token.substring(0, mIdx),
                token.substring(mIdx + 1)
            );
          })
          .collect(Collectors.toMap(
              Pair::getKey,
              Pair::getValue)
          );
    }
  }
}
