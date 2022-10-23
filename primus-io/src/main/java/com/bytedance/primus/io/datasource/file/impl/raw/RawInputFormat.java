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

package com.bytedance.primus.io.datasource.file.impl.raw;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RawInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(RawInputFormat.class);

  public static class RawRecordReader implements RecordReader<NullWritable, BytesWritable> {

    final Path inputPath;
    final JobConf job;
    final Reporter reporter;
    FSDataInputStream rawIn;
    InputStream in;
    final long totalLength;
    final BytesWritable val;
    final CompressionCodec codec;

    public RawRecordReader(FileSplit split, JobConf job,
                           Reporter reporter) throws IOException {
      this.inputPath = split.getPath();
      this.totalLength = split.getLength();
      this.job = job;
      this.reporter = reporter;

      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
      codec = compressionCodecs.getCodec(this.inputPath);

      this.rawIn = FileSystem.get(job).open(inputPath);
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        in = new DataInputStream(codec.createInputStream(this.rawIn, decompressor));
      } else {
        this.in = this.rawIn;
      }
      this.val = new BytesWritable(new byte[65536]);
    }

    @Override
    public boolean next(NullWritable key, BytesWritable value) throws IOException {
      int read = this.in.read(val.getBytes(), 0, val.getCapacity());
      if (read <= 0) {
        return false;
      }
      val.setSize(read);
      return true;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public BytesWritable createValue() {
      return val;
    }

    @Override
    public long getPos() throws IOException {
      return rawIn.getPos();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public float getProgress() throws IOException {
      return ((float) rawIn.getPos()) / totalLength;
    }
  }

  @Override
  public RecordReader<NullWritable, BytesWritable> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter)
      throws IOException {
    return new RawRecordReader((FileSplit) split, job, reporter);
  }
}
