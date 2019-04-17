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

package com.bytedance.primus.webapp;

import com.bytedance.primus.common.exceptions.PrimusRuntimeException;
import com.bytedance.primus.common.util.StringUtils;
import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryContext {

  private static final Logger LOG = LoggerFactory.getLogger(HistoryContext.class);
  private static final int CACHE_SIZE = 4096;

  private final LRUMap snapshotCache;
  private final Path basePath;
  private final FileSystem fs;

  public HistoryContext(Configuration conf, String hdfsBasePath) throws IOException {
    this.basePath = new Path(hdfsBasePath);
    this.snapshotCache = new LRUMap(CACHE_SIZE);
    this.fs = FileSystem.get(conf);
  }

  private Optional<HistorySnapshot> loadSnapshot(Path absoluteSnapshotFilePath) {
    // Lookup cache
    synchronized (snapshotCache) {
      HistorySnapshot cached = (HistorySnapshot) snapshotCache.get(absoluteSnapshotFilePath);
      if (cached != null) {
        return Optional.of(cached);
      }
    }

    // Fetch snapshot from storage
    HistorySnapshot snapshot;
    try (InputStream in = fs.open(absoluteSnapshotFilePath)) {
      snapshot = (HistorySnapshot) HdfsStore
          .getKryo()
          .readClassAndObject(new Input(in, 81920));

    } catch (Exception e) {
      LOG.error("Failed to load " + absoluteSnapshotFilePath, e);
      return Optional.empty();
    }

    // Update cache
    if (snapshot.isFinished()) {
      synchronized (snapshotCache) {
        snapshotCache.put(absoluteSnapshotFilePath, snapshot);
      }
    }

    return Optional.of(snapshot);
  }

  private Optional<HistorySnapshot> getSnapshot(
      Path snapshotDirectory,
      String snapshotFilename
  ) throws IOException {
    Path absoluteSnapshotFilePath = new Path(snapshotDirectory, snapshotFilename);

    long timestamp = System.currentTimeMillis();
    Optional<HistorySnapshot> snapshot = loadSnapshot(
        new Path(snapshotDirectory, snapshotFilename));

    if (!snapshot.isPresent()) {
      LOG.warn("Snapshot {} get snapshot failed, cost {} ms.",
          absoluteSnapshotFilePath,
          System.currentTimeMillis() - timestamp);
    } else {
      LOG.info("Snapshot {} get snapshot success, cost {} ms.",
          absoluteSnapshotFilePath,
          System.currentTimeMillis() - timestamp);
    }

    return snapshot;
  }

  private Path getSnapshotDirectory(String appId) {
    Path path = new Path(basePath, appId);
    try {
      if (fs.getFileStatus(path).isDirectory()) {
        return path;
      }
    } catch (IOException e) {
      LOG.warn("Failed to check " + path);
    }
    return basePath;
  }

  public Optional<HistorySnapshot> getSnapshot(String appId) {
    try {
      Path snapshotDirectory = getSnapshotDirectory(appId);

      // Get the filename of the file with the largest suffix (filename_suffix);
      String snapshotFilename = Arrays
          .stream(fs.globStatus(new Path(snapshotDirectory, "*")))
          .filter(status -> status.isFile() && !status.getPath().getName().endsWith("tmp"))
          .reduce((a, b) -> {
            try {
              int valA = Integer.parseInt(StringUtils.getLastToken(a.getPath().getName(), "_"));
              int valB = Integer.parseInt(StringUtils.getLastToken(b.getPath().getName(), "_"));
              return valA > valB ? a : b;
            } catch (Exception e) {
              LOG.warn("Failed to destruct {}, {}, fall back to modification time", a, b);
              return a.getModificationTime() > b.getModificationTime() ? a : b;
            }
          })
          .map(fileStatus -> fileStatus.getPath().getName())
          .orElseThrow(() -> new PrimusRuntimeException("Missing history snapshot file"));

      return getSnapshot(
          snapshotDirectory,
          snapshotFilename);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<HistorySnapshot> getSnapshot(String appId, String attemptId) {
    try {
      return getSnapshot(getSnapshotDirectory(appId), attemptId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getAppId(String filename) {
    int underline = 0;
    int lastUnderlineIndex = 0;
    String appId = null;
    try {
      for (int i = 0; i < filename.length(); i++) {
        if (filename.charAt(i) == '_') {
          underline += 1;
          lastUnderlineIndex = i;
        }
      }
      if (underline == 3) {
        appId = filename.substring(0, lastUnderlineIndex);
      } else {
        appId = filename;
      }
    } catch (Exception e) {
      LOG.warn("Skip exception", e);
      return filename;
    }
    return appId;
  }

  public List<HistorySnapshot> listRecentApps(int max) {
    List<HistorySnapshot> result = new LinkedList<>();
    List<LocatedFileStatus> allFiles = new LinkedList<>();
    try {
      RemoteIterator<LocatedFileStatus> it = fs.listFiles(basePath, true);
      // Filter tmp files and duplicated application; Keep the most recent attempt
      Map<String, LocatedFileStatus> apps = new HashMap<>();  // appId -> appId_{attemptId}
      while (it.hasNext()) {
        LocatedFileStatus locatedFileStatus = it.next();
        String filename = locatedFileStatus.getPath().getName();
        if (filename.endsWith("tmp")) {
          continue;
        }

        String appId = getAppId(filename);
        LocatedFileStatus lfs = apps.get(appId);
        if (lfs == null) {
          allFiles.add(locatedFileStatus);
          apps.put(appId, locatedFileStatus);
        } else if (filename.compareTo(lfs.getPath().getName()) > 0) {
          allFiles.remove(lfs);
          allFiles.add(locatedFileStatus);
          apps.put(appId, lfs);
        }
      }
      Collections.sort(allFiles,
          (o1, o2) -> Long.compare(o2.getModificationTime(), o1.getModificationTime()));
      for (int i = 0; i < Math.min(max, allFiles.size()); i++) {
        LocatedFileStatus f = allFiles.get(i);
        String filename = f.getPath().getName();
        LOG.info("HistoryContext.listRecentApps: {}", filename);
        Optional<HistorySnapshot> snapshot =
            getSnapshot(getSnapshotDirectory(getAppId(filename)), filename);

        snapshot.ifPresent(result::add);
      }
    } catch (IOException e) {
      LOG.error("", e);
      //throw new RuntimeException(e);
    }
    return result;
  }
}


