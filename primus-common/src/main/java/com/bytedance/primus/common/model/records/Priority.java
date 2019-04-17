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

import com.bytedance.primus.common.model.records.impl.pb.PriorityPBImpl;

/**
 * The priority assigned to a ResourceRequest or Application or Container allocation
 */


public abstract class Priority implements Comparable<Priority> {

  public static final Priority UNDEFINED = newInstance(-1);


  public static Priority newInstance(int p) {
    Priority priority = new PriorityPBImpl();
    priority.setPriority(p);
    return priority;
  }

  /**
   * Get the assigned priority
   *
   * @return the assigned priority
   */


  public abstract int getPriority();

  /**
   * Set the assigned priority
   *
   * @param priority the assigned priority
   */


  public abstract void setPriority(int priority);

  @Override
  public int hashCode() {
    final int prime = 517861;
    int result = 9511;
    result = prime * result + getPriority();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Priority other = (Priority) obj;
    if (getPriority() != other.getPriority()) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Priority other) {
    return other.getPriority() - this.getPriority();
  }

  @Override
  public String toString() {
    return "{Priority: " + getPriority() + "}";
  }
}
