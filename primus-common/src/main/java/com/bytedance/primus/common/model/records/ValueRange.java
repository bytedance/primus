/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */
package com.bytedance.primus.common.model.records;

import com.bytedance.primus.common.model.records.impl.pb.ValueRangePBImpl;

public abstract class ValueRange implements Comparable<ValueRange> {

  public abstract int getBegin();

  public abstract int getEnd();

  public abstract void setBegin(int value);

  public abstract void setEnd(int value);

  public abstract boolean isLessOrEqual(ValueRange other);

  public static ValueRange newInstance(int begin, int end) {
    ValueRange valueRange = new ValueRangePBImpl();
    valueRange.setBegin(begin);
    valueRange.setEnd(end);
    return valueRange;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (getBegin() == getEnd()) {
      result.append(getBegin());
    } else {
      result.append("[" + getBegin() + "-" + getEnd() + "]");
    }
    return result.toString();
  }

  @Override
  public int compareTo(ValueRange other) {
    if (other == null) {
      return -1;
    }

    if (getBegin() == other.getBegin() && getEnd() == other.getEnd()) {
      return 0;
    } else if (getBegin() - other.getBegin() < 0) {
      return -1;
    } else if (getBegin() - other.getBegin() == 0
        && getEnd() - other.getEnd() < 0) {
      return -1;
    } else {
      return 1;
    }

  }

  @Override
  public ValueRange clone() {
    return ValueRange.newInstance(getBegin(), getEnd());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ValueRange)) {
      return false;
    }
    ValueRange other = (ValueRange) obj;
    if (getBegin() == other.getBegin() && getEnd() == other.getEnd()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 263167;
    int result = 0;
    result = prime * result + this.getBegin();
    result = prime * result + this.getEnd();
    return result;
  }
}