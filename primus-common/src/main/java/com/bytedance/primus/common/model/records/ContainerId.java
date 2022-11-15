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

import com.bytedance.primus.common.model.records.impl.pb.ContainerIdPBImpl;
import com.google.common.base.Splitter;
import java.text.NumberFormat;
import java.util.Iterator;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

/**
 * <p><code>ContainerId</code> represents a globally unique identifier
 * for a {@link Container} in the cluster.</p>
 */

// TODO: Remove or streamline this class to further decouple from YARN.
public abstract class ContainerId implements Comparable<ContainerId> {

  public static final long CONTAINER_ID_BITMASK = 0xffffffffffL;
  private static final Splitter _SPLITTER = Splitter.on('_').trimResults();
  private static final String CONTAINER_PREFIX = "container";
  private static final String EPOCH_PREFIX = "e";


  public static ContainerId newContainerId(ApplicationAttemptId appAttemptId,
      long containerId) {
    return newContainerId(appAttemptId, containerId, 0);
  }


  public static ContainerId newContainerId(ApplicationAttemptId appAttemptId,
      long containerId, int migration) {
    ContainerId id = new ContainerIdPBImpl();
    id.setContainerId(containerId);
    id.setApplicationAttemptId(appAttemptId);
    id.setMigration(migration);
    id.build();
    return id;
  }


  @Deprecated

  public static ContainerId newInstance(ApplicationAttemptId appAttemptId,
      int containerId) {
    return newInstance(appAttemptId, containerId, 0);
  }


  @Deprecated

  public static ContainerId newInstance(ApplicationAttemptId appAttemptId,
      int containerId, int migration) {
    ContainerId id = new ContainerIdPBImpl();
    id.setContainerId(containerId);
    id.setApplicationAttemptId(appAttemptId);
    id.setMigration(migration);
    id.build();
    return id;
  }

  public ContainerId getMigrationNextContainerId() {
    ContainerId id = new ContainerIdPBImpl();
    id.setApplicationAttemptId(getApplicationAttemptId());
    id.setContainerId(getContainerId());
    id.setMigration(getMigration() + 1);
    id.build();
    return id;
  }

  public ContainerId getMigrationInitContainerId() {
    ContainerId id = new ContainerIdPBImpl();
    id.setApplicationAttemptId(getApplicationAttemptId());
    id.setContainerId(getContainerId());
    id.setMigration(0);
    id.build();
    return id;
  }

  /**
   * Get the <code>ApplicationAttemptId</code> of the application to which the
   * <code>Container</code> was assigned.
   * <p>
   * Note: If containers are kept alive across application attempts via {@link
   * ApplicationSubmissionContext#setKeepContainersAcrossApplicationAttempts(boolean)} the
   * <code>ContainerId</code> does not necessarily contain the current running application
   * attempt's
   * <code>ApplicationAttemptId</code> This container can be allocated by previously exited
   * application attempt and managed by the current running attempt thus have the previous
   * application attempt's <code>ApplicationAttemptId</code>.
   * </p>
   *
   * @return <code>ApplicationAttemptId</code> of the application to which the
   * <code>Container</code> was assigned
   */


  public abstract ApplicationAttemptId getApplicationAttemptId();


  protected abstract void setApplicationAttemptId(ApplicationAttemptId atId);

  /**
   * Get the lower 32 bits of identifier of the <code>ContainerId</code>, which doesn't include
   * epoch. Note that this method will be marked as deprecated, so please use
   * <code>getContainerId</code> instead.
   *
   * @return lower 32 bits of identifier of the <code>ContainerId</code>
   */

  @Deprecated

  public abstract int getId();

  /**
   * Get the identifier of the <code>ContainerId</code>. Upper 24 bits are reserved as epoch of
   * cluster, and lower 40 bits are reserved as sequential number of containers.
   *
   * @return identifier of the <code>ContainerId</code>
   */


  public abstract long getContainerId();


  protected abstract void setContainerId(long id);


  public abstract int getMigration();


  protected abstract void setMigration(int migration);


  // TODO: fail the app submission if attempts are more than 10 or something
  private static final ThreadLocal<NumberFormat> appAttemptIdAndEpochFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };
  // TODO: Why thread local?
  // ^ NumberFormat instances are not threadsafe
  private static final ThreadLocal<NumberFormat> containerIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  @Override
  public int hashCode() {
    // Generated by IntelliJ IDEA 13.1.
    int result = (int) (getContainerId() ^ (getContainerId() >>> 32));
    result = 31 * result + getApplicationAttemptId().hashCode();
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
    ContainerId other = (ContainerId) obj;
    if (!this.getApplicationAttemptId().equals(other.getApplicationAttemptId())) {
      return false;
    }
    if (this.getContainerId() != other.getContainerId()) {
      return false;
    }
    if (this.getMigration() != other.getMigration()) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(ContainerId other) {
    if (this.getApplicationAttemptId().compareTo(
        other.getApplicationAttemptId()) == 0) {
      int flag = Long.valueOf(getContainerId())
          .compareTo(Long.valueOf(other.getContainerId()));
      if (flag == 0) {
        return Integer.valueOf(this.getMigration())
            .compareTo(Integer.valueOf(other.getMigration()));
      } else {
        return flag;
      }
    } else {
      return this.getApplicationAttemptId().compareTo(
          other.getApplicationAttemptId());
    }
  }

  public boolean checkSame(ContainerId other) {
    if (other == null) {
      return false;
    }
    if (!this.getApplicationAttemptId().equals(other.getApplicationAttemptId())) {
      return false;
    }
    if (this.getContainerId() != other.getContainerId()) {
      return false;
    }
    return true;
  }

  /**
   * @return A string representation of containerId. The format is container_e*epoch*_*clusterTimestamp*_*appId*_*attemptId*_*containerId*
   * when epoch is larger than 0 (e.g. container_e17_1410901177871_0001_01_000005). *epoch* is
   * increased when RM restarts or fails over. When epoch is 0, epoch is omitted (e.g.
   * container_1410901177871_0001_01_000005).
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(CONTAINER_PREFIX + "_");
    long epoch = getContainerId() >> 40;
    if (epoch > 0) {
      sb.append(EPOCH_PREFIX)
          .append(appAttemptIdAndEpochFormat.get().format(epoch)).append("_");
      ;
    }
    ApplicationId appId = getApplicationAttemptId().getApplicationId();
    sb.append(appId.getClusterTimestamp()).append("_");
    sb.append(ApplicationId.appIdFormat.get().format(appId.getId()))
        .append("_");
    sb.append(
        appAttemptIdAndEpochFormat.get().format(
            getApplicationAttemptId().getAttemptId())).append("_");
    sb.append(containerIdFormat.get()
        .format(CONTAINER_ID_BITMASK & getContainerId()));
    return sb.toString();
  }


  public static ContainerId fromString(String containerIdStr) {
    String[] items = containerIdStr.split("_");
    if (items.length < 5) {
      throw new IllegalArgumentException("Invalid ContainerId: "
          + containerIdStr);
    }
    if (items[1].startsWith(EPOCH_PREFIX)) {
      if (items.length == 6) {
        return fromNormalString(containerIdStr);
      } else if (items.length == 7) {
        return fromMigrationString(containerIdStr);
      } else {
        throw new IllegalArgumentException("Invalid ContainerId: "
            + containerIdStr);
      }
    } else {
      if (items.length == 5) {
        return fromNormalString(containerIdStr);
      } else if (items.length == 6) {
        return fromMigrationString(containerIdStr);
      } else {
        throw new IllegalArgumentException("Invalid ContainerId: "
            + containerIdStr);
      }
    }
  }

  /**
   * @Todo liuwei.find add next two functions in this issue
   */
  public String toMigrationString() {
    return toString() + "_" + getMigration();
  }

  public static ContainerId fromMigrationString(String migrationContainerIdStr) {
    int flag = migrationContainerIdStr.lastIndexOf("_");
    if (flag < 0) {
      throw new IllegalArgumentException("Invalid ContainerId: "
          + migrationContainerIdStr);
    }
    String containerIdStr = migrationContainerIdStr
        .substring(0, flag);
    ContainerId containerIdPrefix = fromNormalString(containerIdStr);
    ContainerId containerId = ContainerId.newContainerId(
        containerIdPrefix.getApplicationAttemptId(),
        containerIdPrefix.getContainerId(),
        Integer.valueOf(migrationContainerIdStr.substring(flag + 1)));
    return containerId;
  }

  public static ContainerId fromNormalString(String containerIdStr) {
    Iterator<String> it = _SPLITTER.split(containerIdStr).iterator();
    if (!it.next().equals(CONTAINER_PREFIX)) {
      throw new IllegalArgumentException("Invalid ContainerId prefix: "
          + containerIdStr);
    }
    try {
      String epochOrClusterTimestampStr = it.next();
      long epoch = 0;
      ApplicationAttemptId appAttemptID = null;
      if (epochOrClusterTimestampStr.startsWith(EPOCH_PREFIX)) {
        String epochStr = epochOrClusterTimestampStr;
        epoch = Integer.parseInt(epochStr.substring(EPOCH_PREFIX.length()));
        appAttemptID = toApplicationAttemptId(it);
      } else {
        String clusterTimestampStr = epochOrClusterTimestampStr;
        long clusterTimestamp = Long.parseLong(clusterTimestampStr);
        appAttemptID = toApplicationAttemptId(clusterTimestamp, it);
      }
      long id = Long.parseLong(it.next());
      long cid = (epoch << 40) | id;
      ContainerId containerId = ContainerId.newContainerId(appAttemptID, cid);
      return containerId;
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid ContainerId: "
          + containerIdStr, n);
    }
  }

  private static ApplicationAttemptId toApplicationAttemptId(
      Iterator<String> it) throws NumberFormatException {
    return toApplicationAttemptId(Long.parseLong(it.next()), it);
  }

  private static ApplicationAttemptId toApplicationAttemptId(
      long clusterTimestamp, Iterator<String> it) throws NumberFormatException {
    ApplicationId appId = ApplicationId.newInstance(clusterTimestamp,
        Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, Integer.parseInt(it.next()));
    return appAttemptId;
  }

  protected abstract void build();
}
