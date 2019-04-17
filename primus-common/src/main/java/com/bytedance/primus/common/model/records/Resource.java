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

import com.bytedance.primus.common.model.records.impl.pb.ResourcePBImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * <p><code>Resource</code> models a set of computer resources in the
 * cluster.</p>
 *
 * <p>Currently it models both <em>memory</em> and <em>CPU</em>.</p>
 *
 * <p>The unit for memory is megabytes. CPU is modeled with virtual cores
 * (vcores), a unit for expressing parallelism. A node's capacity should be configured with virtual
 * cores equal to its number of physical cores. A container should be requested with the number of
 * cores it can saturate, i.e. the average number of threads it expects to have runnable at a
 * time.</p>
 *
 * <p>Virtual cores take integer values and thus currently CPU-scheduling is
 * very coarse.  A complementary axis for CPU requests that represents processing power will likely
 * be added in the future to enable finer-grained resource configuration.</p>
 *
 * <p>Typically, applications request <code>Resource</code> of suitable
 * capability to run their component tasks.</p>
 * <p>
 * see ResourceRequest see ApplicationMasterProtocol#allocate(com.bytedance.primus.common.api.protocolrecords.AllocateRequest)
 */


public abstract class Resource implements Comparable<Resource> {

  protected ResourceInformation[] resources = null;
  protected int memoryIndex = -1;
  protected int vcoresIndex = -1;

  public static Resource newInstance(Resource resource) {
    return new ResourcePBImpl(
        resource.getMemoryIndex(),
        resource.getVcoresIndex(),
        resource.getResources());
  }

  public int getMemoryIndex() {
    return memoryIndex;
  }

  public int getVcoresIndex() {
    return vcoresIndex;
  }

  public abstract long getMemory();

  public abstract void setMemory(long memory);

  public abstract int getVirtualCores();

  public abstract void setVirtualCores(int vCores);

  public ResourceInformation[] getResources() {
    return resources;
  }

  public List<ResourceInformation> getAllResourcesListCopy() {
    List<ResourceInformation> list = new ArrayList<>();
    for (ResourceInformation i : resources) {
      ResourceInformation ri = new ResourceInformation();
      ResourceInformation.copy(i, ri);
      list.add(ri);
    }
    return list;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Resource)) {
      return false;
    }

    // Check resource list
    Resource other = (Resource) obj;
    ResourceInformation[] otherVectors = other.getResources();
    if (resources.length != otherVectors.length) {
      return false;
    }
    for (int i = 0; i < resources.length; i++) {
      if (!Objects.equals(resources[i], otherVectors[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareTo(Resource other) {
    ResourceInformation[] otherResources = other.getResources();

    int arrLenThis = this.resources.length;
    int arrLenOther = otherResources.length;

    // compare memory and vcores first(in that order) to preserve
    // existing behavior.
    for (int i = 0; i < arrLenThis; i++) {
      ResourceInformation otherEntry;
      try {
        otherEntry = otherResources[i];
      } catch (ArrayIndexOutOfBoundsException e) {
        // For two vectors with different size and same prefix. Shorter vector
        // goes first.
        return 1;
      }
      ResourceInformation entry = resources[i];

      long diff = entry.compareTo(otherEntry);
      if (diff > 0) {
        return 1;
      } else if (diff < 0) {
        return -1;
      }
    }

    if (arrLenThis < arrLenOther) {
      return -1;
    }

    return 0;
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("<");

    for (ResourceInformation ri : resources) {
      if (ri.getValue() == 0) {
        continue;
      }

      sb.append(", ")
          .append(ri.getName())
          .append(": ")
          .append(ri.getValue()).append(ri.getUnits());

      if (ri.getRanges() != null) {
        sb.append(" { ");
        sb.append(ri.getRanges());
        sb.append(" }");
      }
    }

    return sb.append(">").toString();
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    int result = 0;
    for (ResourceInformation entry : resources) {
      result = prime * result + entry.hashCode();
    }
    return result;
  }

  /**
   * Convert long to int for a resource value safely. This method assumes resource value is
   * positive.
   *
   * @param value long resource value
   * @return int resource value
   */
  protected static int castToIntSafely(long value) {
    if (value > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return Long.valueOf(value).intValue();
  }
}
