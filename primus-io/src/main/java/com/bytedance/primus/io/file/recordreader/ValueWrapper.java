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

package com.bytedance.primus.io.file.recordreader;

import com.bytedance.primus.io.InstanceType;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ValueWrapper implements Writable {

  private Writable realValue;
  private String source;
  private InstanceType instanceType;

  public void setRealValue(Writable realValue) {
    this.realValue = realValue;
  }

  public Writable getRealValue() {
    return realValue;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getSource() {
    return source;
  }

  public InstanceType getInstanceType() {
    return instanceType;
  }

  public void setInstanceType(InstanceType instanceType) {
    this.instanceType = instanceType;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    realValue.write(dataOutput);
    dataOutput.writeUTF(source);
    dataOutput.writeInt(instanceType.value());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    realValue.readFields(dataInput);
    source = dataInput.readUTF();
    instanceType = InstanceType.valueOf(dataInput.readInt());
  }
}
