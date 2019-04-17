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

package com.bytedance.primus.common.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PrimusMetrics {

  private static final TimerMetric emptyTimerMetric = new TimerMetric(null);
  private static MetricRegistry metrics = new MetricRegistry();
  private static MetricsSink sink = null;
  private static ConcurrentHashMap<String, AtomicLong> storeValues = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, AtomicDouble> storeValuesDouble = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, Timer> timerValues = new ConcurrentHashMap<>();
  private static String optionalPrefix;

  // TODO: Create init(String optionalPrefix, RuntimeConf conf) instead.
  public static void init(String optionalPrefix) {
    PrimusMetrics.optionalPrefix = optionalPrefix;
    sink = new LogSink(metrics);
  }

  public static void init(String optionalPrefix, String host, int port, String jobName) {
    PrimusMetrics.optionalPrefix = optionalPrefix;

    sink = new PrometheusPushGatewaySink(host, port, jobName, metrics);
    sink.start();
  }

  public static SortedMap<String, Timer> getTimers(String pattern) {
    PrimusMetricFilter filter = new PrimusMetricFilter(pattern);
    return metrics.getTimers(filter);
  }

  public static SortedMap<String, Counter> getCounters(String pattern) {
    PrimusMetricFilter filter = new PrimusMetricFilter(pattern);
    return metrics.getCounters(filter);
  }

  public static SortedMap<String, Gauge> getGauges(String pattern) {
    PrimusMetricFilter filter = new PrimusMetricFilter(pattern);
    return metrics.getGauges(filter);
  }

  public static long getGauge(String key) {
    return storeValues.get(key).get();
  }

  public static void emitCounterWithOptionalPrefix(String name, long count) {
    emitCounter(optionalPrefix + name, count);
  }

  public static void emitCounter(String name, long count) {
    if (sink == null) {
      return;
    }
    metrics.counter(name).inc(count);
  }

  public static void emitStoreWithOptionalPrefix(String name, int count) {
    emitStore(optionalPrefix + name, count);
  }

  public static void emitStore(String name, int count) {
    if (sink == null) {
      return;
    }
    storeValues.computeIfAbsent(name, k -> {
          final AtomicLong newGauge = new AtomicLong(0);
          metrics.register(name, (Gauge) () -> newGauge.longValue());
          return newGauge;
        }
    );
    AtomicLong gauge = storeValues.get(name);
    gauge.set(count);
  }

  public static void emitStoreWithOptionalPrefix(String name, double count) {
    emitStore(optionalPrefix + name, count);
  }

  public static void emitStore(String name, double count) {
    if (sink == null) {
      return;
    }
    storeValuesDouble.computeIfAbsent(
        name,
        k -> {
          final AtomicDouble newGauge = new AtomicDouble(0);
          metrics.register(name, (Gauge<Double>) newGauge::doubleValue);
          return newGauge;
        });
    AtomicDouble gauge = storeValuesDouble.get(name);
    gauge.set(count);
  }

  public static TimerMetric getTimerContextWithOptionalPrefix(String name) {
    return getTimerContext(optionalPrefix + name);
  }

  public static TimerMetric getTimerContext(String name) {
    if (sink == null) {
      return emptyTimerMetric;
    }
    Timer t = timerValues.computeIfAbsent(name, k -> {
      Timer timer = new Timer();
      metrics.register(name, timer);
      return timer;
    });
    return new TimerMetric(t.time());
  }


  public static class TimerMetric {

    private Timer.Context context;

    public TimerMetric(Timer.Context context) {
      this.context = context;
    }

    public long stop() {
      if (context != null) {
        return context.stop();
      }
      return 0;
    }
  }

  public static class PrimusMetricFilter implements MetricFilter {

    private String pattern;

    public PrimusMetricFilter(String pattern) {
      this.pattern = pattern;
    }

    @Override
    public boolean matches(String name, Metric metric) {
      return name.contains(pattern);
    }
  }

  public static String prefixWithSingleTag(String prefix, String tag, String value) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(prefix).append("{").append(tag).append("=").append(value).append("}");
    return stringBuilder.toString();
  }
}
