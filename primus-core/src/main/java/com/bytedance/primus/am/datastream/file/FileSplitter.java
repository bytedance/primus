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

package com.bytedance.primus.am.datastream.file;

import com.bytedance.primus.am.AMContext;
import com.bytedance.primus.am.ApplicationExitCode;
import com.bytedance.primus.am.ApplicationMasterEvent;
import com.bytedance.primus.am.ApplicationMasterEventType;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.io.datasource.file.FileDataSource;
import com.bytedance.primus.io.datasource.file.models.BaseInput;
import com.bytedance.primus.io.datasource.file.models.BaseSplit;
import com.bytedance.primus.io.datasource.file.models.PrimusInput;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSplitter extends Thread {

  private static final int IO_RETRY_TIMES = 3;

  private final Logger LOG;
  private final AMContext context;
  private final FileSystem fileSystem;

  private final FileTaskBuilder builder;
  private final FileScanner scanner;

  private final int numThreads;
  private final BlockingQueue<Future<List<BaseSplit>>> splitsBlockingQueue;

  private static String getLoggerName(FileTaskBuilder builder) {
    return FileSplitter.class.getName() + "[" + builder.getName() + "]";
  }

  public FileSplitter(AMContext context, FileTaskBuilder builder, int numThreads) {
    super(getLoggerName(builder));
    setDaemon(true);

    this.LOG = LoggerFactory.getLogger(getLoggerName(builder));
    this.context = context;
    this.fileSystem = context.getHadoopFileSystem();

    this.builder = builder;
    this.scanner = builder.getFileScanner();
    this.numThreads = numThreads;
    this.splitsBlockingQueue = new LinkedBlockingQueue<>(numThreads * 10000); // avoid OOM
  }

  public boolean isEmpty() {
    return splitsBlockingQueue.isEmpty();
  }

  public Future<List<BaseSplit>> poll(
      long timeout,
      TimeUnit unit
  ) throws InterruptedException {
    return splitsBlockingQueue.poll(timeout, unit);
  }

  private void failApplication(String diag, int exitCode) {
    LOG.error(diag);
    context.getDispatcher()
        .getEventHandler()
        .handle(new ApplicationMasterEvent(
            context,
            ApplicationMasterEventType.FAIL_APP,
            diag,
            exitCode
        ));
  }

  private List<BaseSplit> getFileSplits(FileSystem fileSystem, BaseInput input) {
    TimerMetric latency = PrimusMetrics.getTimerContextWithAppIdTag(
        "am.taskbuilder.splitter.latency", new HashMap<>());

    List<BaseSplit> result = new LinkedList<>();
    int ioExceptions = 0;
    while (true) {
      try {
        if (input instanceof PrimusInput) {
          PrimusInput primusInput = (PrimusInput) input;
          FileDataSource source = FileDataSource.load(primusInput.getSpec());
          result = new ArrayList<>(source.scanPattern(fileSystem, primusInput));
        }
        break;
      } catch (AccessControlException e) {
        // GDPR
        String diag = "Failed to get splits for input [" + input + "], fail job because of " + e;
        failApplication(diag, ApplicationExitCode.GDPR.getValue());
      } catch (IllegalArgumentException e) {
        // Wrong FS
        String diag = "Failed to get splits for input [" + input + "], fail job because of " + e;
        failApplication(diag, ApplicationExitCode.WRONG_FS.getValue());
      } catch (NoSuchFileException e) {
        LOG.warn("Skip input " + input, e);
        break;
      } catch (IOException e) {
        LOG.warn("Failed to get file splits for " + input + ", retry", e);
        if (ioExceptions ++ > IO_RETRY_TIMES) {
          LOG.warn("Failed to get file splits for " + input + ", skip it", e);
          break;
        }
      } catch (Exception e) {
        LOG.error("Failed to get file splits for " + input + ", throw exception", e);
        throw e;
      }
    }

    latency.stop();
    return result;
  }

  private List<BaseSplit> getFileSplits(FileSystem fileSystem, List<BaseInput> inputs) {
    List<BaseSplit> primusSplits = new LinkedList<>();
    String key = null;
    for (BaseInput input : inputs) {
      // check they have the same key
      if (key == null) {
        key = input.getBatchKey();
      } else {
        assert key.equals(input.getBatchKey());
      }
      primusSplits.addAll(getFileSplits(fileSystem, input));
    }

    return primusSplits;
  }

  @Override
  public void run() {
    ExecutorService pool = Executors.newFixedThreadPool(
        numThreads, new ThreadFactoryBuilder().setDaemon(true).build());

    while (!builder.isStopped()) {
      try {
        // blocking until available
        List<BaseInput> inputs = scanner.takeInputBatch();
        Future<List<BaseSplit>> future = pool.submit(() -> getFileSplits(fileSystem, inputs));
        splitsBlockingQueue.put(future);  // blocking if no capacity, for avoiding OOM
      } catch (InterruptedException interruptedException) {
        LOG.info("Ignore interrupted exception and continue to get inputs");
      } catch (Exception e) {
        failApplication(
            "Failed to get inputs from file scanner and split " + e,
            ApplicationExitCode.BUILD_TASK_FAILED.getValue());
        break;
      }
    }

    pool.shutdownNow();
  }
}
