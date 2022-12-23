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
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.apiserver.records.DataStreamSpec;
import com.bytedance.primus.common.metrics.PrimusMetrics;
import com.bytedance.primus.common.metrics.PrimusMetrics.TimerMetric;
import com.bytedance.primus.common.util.Sleeper;
import com.bytedance.primus.common.util.StringUtils;
import com.bytedance.primus.io.datasource.file.models.BaseInput;
import com.bytedance.primus.io.datasource.file.models.PrimusInput;
import com.bytedance.primus.proto.PrimusCommon.Time.Date;
import com.bytedance.primus.proto.PrimusCommon.Time.DateHour;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import com.bytedance.primus.utils.TimeUtils;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileScanner {

  private static final int DEFAULT_IO_RETRY_TIMES = 3;
  private static final String DEFAULT_BATCH_KEY = "DEFAULT-BATCH-KEY";
  private static final Duration DEFAULT_SCAN_TO_NOW_PERIOD = Duration.ofMinutes(5);

  private final Logger LOG;
  private final AMContext context;
  private final FileSystem fileSystem;
  private final DataStreamSpec dataStreamSpec;

  @Getter
  private volatile boolean isStopped = false;
  @Getter
  private volatile boolean isFinished = false;

  private final Thread scannerThread;
  private final BlockingQueue<List<BaseInput>> inputQueue;

  public FileScanner(AMContext context, String name, DataStreamSpec dataStreamSpec) {
    this.LOG = LoggerFactory.getLogger(FileScanner.class.getName() + "[" + name + "]");
    this.context = context;
    this.fileSystem = context.getApplicationMeta().getHadoopFileSystem();
    this.dataStreamSpec = dataStreamSpec;

    this.inputQueue = new LinkedBlockingQueue<>();
    this.scannerThread = new ScannerThread();
  }

  public void start() {
    this.scannerThread.start();
  }

  public void stop() {
    isStopped = true;
    scannerThread.interrupt();
    try {
      scannerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Stop caught interrupted exception", e);
    }
  }

  public boolean isEmpty() {
    return inputQueue.isEmpty();
  }

  public List<BaseInput> takeInputBatch() throws InterruptedException {
    return inputQueue.take();
  }

  private void processAndAddToQueue(List<BaseInput> inputs) {
    if (!inputs.isEmpty()) {
      inputQueue.add(inputs);
    }
  }

  private void failedApp(String diag, int exitCode) {
    LOG.error(diag);
    context.emitFailApplicationEvent(diag, exitCode);
  }

  class ScannerThread extends Thread {

    public ScannerThread() {
      super(ScannerThread.class.getName());
      setDaemon(true);
    }

    @Override
    public void run() {
      List<FileSourceInput> pendingInputs = dataStreamSpec.getDataSourceSpecs().stream()
          .map(spec -> new FileSourceInput(spec.getProto()))
          .collect(Collectors.toList());

      // Input with TimeRange
      if (isInputWithTimeRange(pendingInputs)) {
        try {
          scanTimeRangedInputs(pendingInputs);
        } catch (ParseException e) {
          failedApp(
              String.format("Failed to iterate pendingInputs: %s)", e),
              ApplicationExitCode.DATA_INCOMPATIBLE.getValue());
        }
        isFinished = true;
        return;
      }

      // Input from directory
      scanSingleBatchInputs(pendingInputs);
      isFinished = true;
    }

    private boolean isInputWithTimeRange(List<FileSourceInput> fileSourceInputs) {
      Optional<FileSourceInput> first = fileSourceInputs.stream().findFirst();
      if (!first.isPresent()) {
        return false;
      }
      boolean withTimeRange = first.get().getTimeRange().isPresent();
      for (FileSourceInput input : fileSourceInputs) {
        Preconditions.checkArgument(withTimeRange == input.getTimeRange().isPresent());
      }
      return withTimeRange;
    }

    private void scanSingleBatchInputs(List<FileSourceInput> fileSourceInputs) {
      List<BaseInput> inputs = fileSourceInputs.stream()
          .map(input -> generatePrimusInput(
              DEFAULT_BATCH_KEY, // Constant BatchKey to allow shuffling among DataSources.
              input,
              new HashMap<>()
          ))
          .collect(Collectors.toList());

      processAndAddToQueue(inputs);
    }

    private void scanTimeRangedInputs(List<FileSourceInput> inputs) throws ParseException {
      TimeRangeIterator iterator = new TimeRangeIterator(inputs);
      while (!isStopped && iterator.prepareNextBatch()) {
        try {
          List<FileSourceInput> fileSourceInputs = iterator.peekNextBatch();
          List<BaseInput> scannedInputs = fileSourceInputs.stream()
              .flatMap(input -> scanSingleTimeRangedInput(iterator.getBatchKey(), input).stream())
              .collect(Collectors.toList());

          processAndAddToQueue(scannedInputs);
          LOG.info("{} input(s) has been enqueued, currentBatchCursor: {}",
              scannedInputs.size(),
              iterator.getBatchKey()
          );

        } catch (RuntimeException e) {
          String diag = "Error when scanning some FileSourceInput: " + e.getMessage();
          LOG.error(diag);
          failedApp(diag, ApplicationExitCode.BUILD_TASK_FAILED.getValue());
        } finally {
          iterator.popNextBatch();
        }
      }
    }

    private List<BaseInput> scanSingleTimeRangedInput(String batchKey, FileSourceInput input) {
      TimerMetric latency = PrimusMetrics.getTimerContextWithAppIdTag(
          "am.filescanner.scan_inputs.latency");

      try {
        if (!input.getTimeRange().isPresent()) {
          throw new IllegalArgumentException("Missing TimeRange: " + input);
        }

        TimeRange timeRange = input.getTimeRange().get();
        switch (timeRange.getFrom().getTimeCase()) {
          case DATE:
            return scanDailyInput(
                batchKey, input,
                timeRange.getFrom().getDate(),
                timeRange.getTo().getDate()
            );
          case DATE_HOUR:
            return scanHourlyInput(
                batchKey, input,
                timeRange.getFrom().getDateHour(),
                timeRange.getTo().getDateHour()
            );
          default:
            throw new IllegalArgumentException("Invalid TimeRange: " + timeRange);
        }

      } catch (IOException e) {
        // TODO: Retry
        LOG.error("Failed to scan TimeRanged input due to IOException: {}", e.getMessage());
        throw new RuntimeException(e);

      } catch (ParseException e) {
        LOG.error("Failed to scan TimeRanged input due to ParseException: {}", e.getMessage());
        throw new RuntimeException(e);

      } finally {
        latency.stop();
      }
    }

    public List<BaseInput> scanDailyInput(
        String batchKey,
        FileSourceInput input,
        Date start,
        Date end
    ) throws ParseException, IOException {

      List<BaseInput> results = new LinkedList<>();
      for (
          Date current = start;
          !TimeUtils.isAfter(current, end);
          current = TimeUtils.plusDay(current, 1)
      ) {
        // Build dictionary
        int numericalDate = current.getDate();
        Map<String, String> dictionary = new HashMap<String, String>() {{
          put("\\{\\{YYYY\\}\\}", String.format("%04d", numericalDate / 10000));
          put("\\{\\{MM\\}\\}", String.format("%02d", numericalDate % 10000 / 100));
          put("\\{\\{DD\\}\\}", String.format("%02d", numericalDate % 100));
        }};

        // Translate patterns in spec
        results.add(generatePrimusInput(batchKey, input, dictionary));
      }

      return results;
    }

    public List<BaseInput> scanHourlyInput(
        String batchKey,
        FileSourceInput input,
        DateHour start,
        DateHour end
    ) throws ParseException, IOException {

      List<BaseInput> results = new LinkedList<>();
      for (
          DateHour current = start;
          !TimeUtils.isAfter(current, end);
          current = TimeUtils.plusHour(current, 1)
      ) {

        // Build dictionary
        int numericalDate = current.getDate();
        int numericalHour = current.getHour();
        Map<String, String> dictionary = new HashMap<String, String>() {{
          put("\\{\\{YYYY\\}\\}", String.format("%04d", numericalDate / 10000));
          put("\\{\\{MM\\}\\}", String.format("%02d", numericalDate % 10000 / 100));
          put("\\{\\{DD\\}\\}", String.format("%02d", numericalDate % 100));
          put("\\{\\{HH\\}\\}", String.format("%02d", numericalHour));
        }};

        // Translate patterns in spec
        results.add(generatePrimusInput(batchKey, input, dictionary));
      }

      return results;
    }

    private PrimusInput generatePrimusInput(
        String batchKey,
        FileSourceInput input,
        Map<String, String> dictionary
    ) {
      try {
        // Translate patterns in spec
        FileSourceSpec spec = input.getSpec();

        String translatedPath = StringUtils
            .genFromTemplateAndDictionary(spec.getPathPattern(), dictionary);
        String translatedName = StringUtils
            .genFromTemplateAndDictionary(spec.getNamePattern(), dictionary);
        String[] translatedSuccessMarkers = spec.getSuccessMarkerPatternsList().stream()
            .map(pattern -> StringUtils.genFromTemplateAndDictionary(pattern, dictionary))
            .toArray(String[]::new);

        // Assemble results
        waitForReadyPath(translatedPath, translatedSuccessMarkers);
        return new PrimusInput(
            input.getSourceId(),
            input.getSource(),
            batchKey,
            new Path(translatedPath, translatedName).toString(),
            input.getSpec()
        );

      } catch (IOException e) {
        throw new RuntimeException("Wrapped IOException: " + e.getMessage(), e);
      }
    }

    /**
     * Wait for the path becoming ready for being scheduled.
     *
     * @param path           the path to the file or directory
     * @param successMarkers files marking whether the path is ready if path points to a directory.
     */
    private void waitForReadyPath(String path, String[] successMarkers) throws IOException {
      int retry = 0;
      while (true) {
        try {
          FileStatus status = fileSystem.getFileStatus(new Path(path));
          if (status.isFile()) {
            return;
          } else if (status.isDirectory()) {
            for (String marker : successMarkers) {
              Path markerPath = new Path(path, marker);
              if (!fileSystem.exists(markerPath)) {
                throw new FileNotFoundException("Waiting for success marker: " + markerPath);
              }
            }
            return;
          } else {
            throw new IOException("Unsupported FileStatus: " + status);
          }

        } catch (FileNotFoundException e) {
          LOG.info("Path({}) is not ready, sleeping for {} secs: {}",
              path, DEFAULT_SCAN_TO_NOW_PERIOD.getSeconds(), e.getMessage());
          Sleeper.sleepWithoutInterruptedException(DEFAULT_SCAN_TO_NOW_PERIOD);

        } catch (AccessControlException e) {
          String diag = "Failed to access: " + path + "]:" + e;
          failedApp(diag, ApplicationExitCode.GDPR.getValue());

        } catch (IOException e) {
          LOG.warn("Failed to scan path({}), but retrying: {}", path, e);
          if (retry++ > DEFAULT_IO_RETRY_TIMES) {
            LOG.error("Failed to scan path({}): {}", path, e);
            throw e;
          }
        }
      }
    }
  }
}
