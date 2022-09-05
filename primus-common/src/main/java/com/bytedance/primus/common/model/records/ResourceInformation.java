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

import com.bytedance.primus.common.model.protocolrecords.ResourceTypes;
import com.bytedance.primus.common.model.util.UnitsConversionUtil;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Class to encapsulate information about a Resource - the name of the resource, the units(milli,
 * micro, etc), the type(countable), and the value.
 */
public class ResourceInformation implements Comparable<ResourceInformation> {

  private String name;
  private String units;
  private ResourceTypes resourceType;
  private long value;
  private ValueRanges ranges;
  private long minimumAllocation;
  private long maximumAllocation;

  // Known resource types
  public static final String MEMORY_URI = "memory-mb";
  public static final String VCORES_URI = "vcores";
  public static final String VCORES_MILLI_URI = "vcores-milli";
  public static final String GPU_URI = "yarn.io/gpu";
  public static final String TPU_URI = "yarn.io/tpu";
  public static final String TPU_V3_BASE_URI = "yarn.io/tpu-v3-base";
  public static final String TPU_V3_POD_URI = "yarn.io/tpu-v3-pod";
  public static final String PORT_URI = "yarn.io/port";

  public static final ResourceInformation MEMORY_MB =
      ResourceInformation.newInstance(MEMORY_URI, "Mi");
  public static final ResourceInformation VCORES =
      ResourceInformation.newInstance(VCORES_URI);
  public static final ResourceInformation VCORES_MILLI =
      ResourceInformation.newInstance(VCORES_MILLI_URI, "m");
  public static final ResourceInformation GPUS =
      ResourceInformation.newInstance(GPU_URI);
  public static final ResourceInformation TPUS_V3_BAS =
      ResourceInformation.newInstance(TPU_V3_BASE_URI);
  public static final ResourceInformation TPUS_V3_POD =
      ResourceInformation.newInstance(TPU_V3_POD_URI);
  public static final ResourceInformation PORTS =
      ResourceInformation.newInstance(PORT_URI);

  public static final Map<String, ResourceInformation> MANDATORY_RESOURCES =
      ImmutableMap.<String, ResourceInformation>builder()
          .put(MEMORY_URI, MEMORY_MB)
          .put(VCORES_URI, VCORES)
          .put(VCORES_MILLI_URI, VCORES_MILLI)
          .put(GPU_URI, GPUS)
          .put(TPU_V3_BASE_URI, TPUS_V3_BAS)
          .put(TPU_V3_POD_URI, TPUS_V3_POD)
          .put(PORT_URI, PORTS)
          .build();

  /**
   * Get the name for the resource.
   *
   * @return resource name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the name for the resource.
   *
   * @param rName name for the resource
   */
  public void setName(String rName) {
    this.name = rName;
  }

  /**
   * Get units for the resource.
   *
   * @return units for the resource
   */
  public String getUnits() {
    return units;
  }

  /**
   * Set the units for the resource.
   *
   * @param rUnits units for the resource
   */
  public void setUnits(String rUnits) {
    if (!UnitsConversionUtil.KNOWN_UNITS.contains(rUnits)) {
      throw new IllegalArgumentException(
          "Unknown unit '" + rUnits + "'. Known units are "
              + UnitsConversionUtil.KNOWN_UNITS);
    }
    this.units = rUnits;
  }

  /**
   * Checking if a unit included by KNOWN_UNITS is an expensive operation. This can be avoided in
   * critical path in RM.
   *
   * @param rUnits units for the resource can be avoided in critical path in RM.
   * @param rUnits units for the resource
   */

  public void setUnitsWithoutValidation(String rUnits) {
    this.units = rUnits;
  }

  /**
   * Get the resource type.
   *
   * @return the resource type
   */
  public ResourceTypes getResourceType() {
    return resourceType;
  }

  /**
   * Set the resource type.
   *
   * @param type the resource type
   */
  public void setResourceType(ResourceTypes type) {
    this.resourceType = type;
  }

  /**
   * Get the value for the resource.
   *
   * @return the resource value
   */
  public long getValue() {
    return value;
  }

  /**
   * Set the value for the resource.
   *
   * @param rValue the resource value
   */
  public void setValue(long rValue) {
    this.value = rValue;
  }

  /**
   * Get the range for the resource
   *
   * @return the resource range
   */
  public ValueRanges getRanges() {
    return ranges;
  }

  /**
   * Set the range for the resource
   *
   * @param rRanges the resource range
   */
  public void setRanges(ValueRanges rRanges) {
    this.ranges = rRanges;
  }

  /**
   * Get the minimum allocation for the resource.
   *
   * @return the minimum allocation for the resource
   */
  public long getMinimumAllocation() {
    return minimumAllocation;
  }

  /**
   * Set the minimum allocation for the resource.
   *
   * @param minimumAllocation the minimum allocation for the resource
   */
  public void setMinimumAllocation(long minimumAllocation) {
    this.minimumAllocation = minimumAllocation;
  }

  /**
   * Get the maximum allocation for the resource.
   *
   * @return the maximum allocation for the resource
   */
  public long getMaximumAllocation() {
    return maximumAllocation;
  }

  /**
   * Set the maximum allocation for the resource.
   *
   * @param maximumAllocation the maximum allocation for the resource
   */
  public void setMaximumAllocation(long maximumAllocation) {
    this.maximumAllocation = maximumAllocation;
  }

  /**
   * Create a new instance of ResourceInformation from another object.
   *
   * @param other the object from which the new object should be created
   * @return the new ResourceInformation object
   */
  public static ResourceInformation newInstance(ResourceInformation other) {
    ResourceInformation ret = new ResourceInformation();
    copy(other, ret);
    return ret;
  }

  public static ResourceInformation newInstance(String name, String units, long value,
      ResourceTypes type,
      long minimumAllocation, long maximumAllocation) {
    return ResourceInformation
        .newInstance(name, units, value, ResourceTypes.COUNTABLE, null, 0L,
            Long.MAX_VALUE);
  }

  public static ResourceInformation newInstance(String name, String units, long value,
      ResourceTypes type,
      ValueRanges ranges, long minimumAllocation, long maximumAllocation) {
    ResourceInformation ret = new ResourceInformation();
    ret.setName(name);
    ret.setResourceType(type);
    ret.setUnits(units);
    ret.setValue(value);
    ret.setRanges(ranges);
    ret.setMinimumAllocation(minimumAllocation);
    ret.setMaximumAllocation(maximumAllocation);
    ret.setMinimumAllocation(minimumAllocation);
    ret.setMaximumAllocation(maximumAllocation);
    return ret;
  }

  public static ResourceInformation newInstance(String name, String units,
      long value) {
    return ResourceInformation
        .newInstance(name, units, value, ResourceTypes.COUNTABLE, 0L,
            Long.MAX_VALUE);
  }

  public static ResourceInformation newInstance(String name, String units) {
    return ResourceInformation
        .newInstance(name, units, 0L, ResourceTypes.COUNTABLE, 0L,
            Long.MAX_VALUE);
  }

  public static ResourceInformation newInstance(String name, long value) {
    return ResourceInformation
        .newInstance(name, "", value, ResourceTypes.COUNTABLE, 0L,
            Long.MAX_VALUE);
  }

  public static ResourceInformation newInstance(String name) {
    return ResourceInformation.newInstance(name, "");
  }

  /**
   * Copies the content of the source ResourceInformation object to the destination object,
   * overwriting all properties of the destination object.
   *
   * @param src Source ResourceInformation object
   * @param dst Destination ResourceInformation object
   */

  public static void copy(ResourceInformation src, ResourceInformation dst) {
    dst.setName(src.getName());
    dst.setResourceType(src.getResourceType());
    dst.setUnits(src.getUnits());
    dst.setValue(src.getValue());
    dst.setRanges(src.getRanges());
    dst.setMinimumAllocation(src.getMinimumAllocation());
    dst.setMaximumAllocation(src.getMaximumAllocation());
  }

  @Override
  public String toString() {
    return "name: " + this.name + ", units: " + this.units + ", type: "
        + resourceType + ", value: " + value + ", ranges: " + ranges
        + ", minimum allocation: " + minimumAllocation
        + ", maximum allocation: " + maximumAllocation;
  }

  public String getShorthandRepresentation() {
    return "" + this.value + this.units;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ResourceInformation)) {
      return false;
    }
    ResourceInformation r = (ResourceInformation) obj;
    if (!this.name.equals(r.getName())
        || !this.resourceType.equals(r.getResourceType())) {
      return false;
    }
    if (this.units.equals(r.units)) {
      return this.value == r.value;
    }
    return (UnitsConversionUtil.compare(this.units, this.value, r.units,
        r.value) == 0);
  }

  @Override
  public int hashCode() {
    final int prime = 263167;
    int result =
        939769357 + name.hashCode(); // prime * result = 939769357 initially
    result = prime * result + resourceType.hashCode();
    result = prime * result + units.hashCode();
    result = prime * result + Long.valueOf(value).hashCode();
    if (ranges != null) {
      result = prime * result + ranges.hashCode();
    }
    return result;
  }

  @Override
  public int compareTo(ResourceInformation other) {
    int diff = this.name.compareTo(other.name);
    if (diff == 0) {
      diff = UnitsConversionUtil
          .compare(this.units, this.value, other.units, other.value);
      if (diff == 0) {
        diff = this.resourceType.compareTo(other.resourceType);
      }
    }
    return diff;
  }
}
