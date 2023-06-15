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

package com.bytedance.primus.apiserver.service.storage;

import com.bytedance.primus.apiserver.proto.ApiServerConfProto.ApiServerConf;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDBStorage implements Storage {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBStorage.class);

  private static final String ROCKSDB_PATH = "__primus_state__";
  private static final String DEFAULT_COLUMN_FAMILY = "default";
  private static final String DELIMITER = "#";

  private int retainVersion;
  private RocksDB rocksDB;
  private Map<String, ColumnFamilyHandle> columnFamilyHandleMap;
  private ReentrantReadWriteLock.WriteLock columnFamilyHandleWriteLock;

  public RocksDBStorage(ApiServerConf conf, int retainVersion) throws RocksDBException {
    String dbPath = getDbPath(conf);
    LOG.info("DB Path: " + dbPath);
    RocksDB.loadLibrary();
    this.retainVersion = retainVersion;

    Options options = new Options();
    List<ColumnFamilyHandle> columnFamilyHandles = new LinkedList<>();
    List<ColumnFamilyDescriptor> columnFamilyDescriptors = new LinkedList<>();
    for (byte[] entry : RocksDB.listColumnFamilies(options, dbPath)) {
      ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(entry);
      columnFamilyDescriptors.add(columnFamilyDescriptor);
      LOG.info("Recover exist column family: " + new String(entry));
    }
    if (columnFamilyDescriptors.isEmpty()) {
      LOG.info("Add a default column family: " + DEFAULT_COLUMN_FAMILY);
      columnFamilyDescriptors.add(new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY.getBytes()));
    }

    DBOptions dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);
    dbOptions.setMaxBackgroundJobs(4);
    columnFamilyHandleMap = new ConcurrentHashMap<>();
    rocksDB = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);
    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
      columnFamilyHandleMap.put(new String(columnFamilyHandle.getName()), columnFamilyHandle);
    }
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    columnFamilyHandleWriteLock = reentrantReadWriteLock.writeLock();

    DataCleaner cleaner = new DataCleaner();
    cleaner.start();
  }

  @Override
  public void put(String kind, String name, long version, byte[] value) throws Exception {
    ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(kind);
    if (columnFamilyHandle == null) {
      try {
        columnFamilyHandleWriteLock.lock();
        if (!columnFamilyHandleMap.containsKey(kind)) {
          ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(kind.getBytes());
          columnFamilyHandle = rocksDB.createColumnFamily(descriptor);
          columnFamilyHandleMap.put(kind, columnFamilyHandle);
        } else {
          columnFamilyHandle = columnFamilyHandleMap.get(kind);
        }
      } finally {
        columnFamilyHandleWriteLock.unlock();
      }
    }
    rocksDB.put(columnFamilyHandle, encodeToStorageKey(name, version), value);
  }

  @Override
  public byte[] get(String kind, String name, long version) throws Exception {
    ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleMap.get(kind);
    if (columnFamilyHandle != null) {
      return rocksDB.get(columnFamilyHandle, encodeToStorageKey(name, version));
    } else {
      throw new NoSuchElementException("there is no kind[" + kind + "], name[" + name + "]");
    }
  }

  private byte[] encodeToStorageKey(String name, long version) {
    String key = name + DELIMITER + version;
    return key.getBytes();
  }

  class DataCleaner extends Thread {

    public DataCleaner() {
      super(DataCleaner.class.getName());
      setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        WriteOptions writeOptions = null;
        WriteBatch writeBatch = null;
        RocksIterator iterator = null;
        try {
          Thread.sleep(15 * 60 * 1000);
          writeOptions = new WriteOptions();
          writeBatch = new WriteBatch();
          for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleMap.values()) {
            iterator = rocksDB.newIterator(columnFamilyHandle);
            Map<String, List<Long>> versionMap = new HashMap<>();
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
              String key = new String(iterator.key());
              String[] items = key.split(DELIMITER);
              versionMap.putIfAbsent(items[0], new LinkedList<>());
              versionMap.get(items[0]).add(Long.valueOf(items[1]));
            }
            for (List<Long> versions : versionMap.values()) {
              Collections.sort(versions);
            }
            for (Map.Entry<String, List<Long>> entry : versionMap.entrySet()) {
              String name = entry.getKey();
              List<Long> versions = entry.getValue();
              for (int i = 0; i < versions.size() - retainVersion; i++) {
                writeBatch.delete(columnFamilyHandle, encodeToStorageKey(name, versions.get(i)));
              }
            }
            rocksDB.write(writeOptions, writeBatch);
          }
        } catch (Throwable throwable) {
          LOG.warn("Clean old data failed", throwable);
        } finally {
          if (writeOptions != null) {
            writeOptions.close();
          }
          if (writeBatch != null) {
            writeBatch.close();
          }
          if (iterator != null) {
            iterator.close();
          }
        }
      }
    }
  }

  private String getDbPath(ApiServerConf conf) {
    if (conf != null && conf.hasStateStore()) {
      if (conf.getStateStore().hasNasStateStore()) {
        return conf.getStateStore().getNasStateStore().getVolume();
      }
    }
    return ROCKSDB_PATH;
  }
}
