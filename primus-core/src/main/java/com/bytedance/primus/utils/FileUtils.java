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

package com.bytedance.primus.utils;


import com.bytedance.primus.am.datastream.file.FileSourceInput;
import com.bytedance.primus.io.datasource.file.models.BaseInput;
import com.bytedance.primus.io.datasource.file.models.PrimusInput;
import com.bytedance.primus.proto.PrimusCommon.DayFormat;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils { // TODO: Rename this class as it's actually serving HDFS

  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  private static boolean isFileExist(FileSystem fileSystem, Path path) throws IOException {
    boolean result = fileSystem.exists(path);
    LOG.info("Check file[" + path + "] exist: " + result);
    return result;
  }

  public static List<BaseInput> scanDayInput(FileSystem fileSystem,
      FileSourceInput fileSourceInput, int startDay, int endDay, DayFormat timeFormat,
      boolean checkSuccess) throws ParseException, IOException {

    List<BaseInput> results = new LinkedList<>();
    for (
        int fileDay = startDay;
        fileDay <= endDay;
        fileDay = Integer.valueOf(TimeUtils.plusDay(fileDay, 1))
    ) {
      String dayPath =
          fileSourceInput.getSpec().getPathPattern() + "/" + getDayPath(fileDay, timeFormat) + "/";
      Path successFilePath = new Path(dayPath + "_SUCCESS");
      String inputPath = dayPath + fileSourceInput.getSpec().getNamePattern();
      if (checkSuccess && !isFileExist(fileSystem, successFilePath)) {
        return null;
      }
      PrimusInput input = new PrimusInput(
          fileSourceInput.getSourceId(),
          fileSourceInput.getSource(),
          String.valueOf(fileDay),
          inputPath,
          fileSourceInput.getSpec());
      results.add(input);
    }
    return results;
  }

  public static String getDayPath(int day, DayFormat timeFormat)
      throws ParseException, IOException {
    String dayFormat = getTimeFormat(timeFormat);
    if (dayFormat.equals(PrimusConstants.DATE_FORMAT_DEFAULT)) {
      return String.valueOf(day);
    }
    SimpleDateFormat defaultDateFormat = new SimpleDateFormat(PrimusConstants.DATE_FORMAT_DEFAULT);
    SimpleDateFormat dateFormat = new SimpleDateFormat(dayFormat);
    if (dayFormat.equals(PrimusConstants.DATE_FORMAT_RANGE)) {
      int endDay = TimeUtils.plusDay(day, 1);
      return dateFormat.format(
          defaultDateFormat.parse(String.valueOf(day))) + "-" +
          dateFormat.format(defaultDateFormat.parse(String.valueOf(endDay)));
    }
    return dateFormat.format(defaultDateFormat.parse(String.valueOf(day)));
  }

  public static String getTimeFormat(DayFormat timeFormat) throws IOException {
    switch (timeFormat) {
      case DEFAULT_DAY:
        return PrimusConstants.DATE_FORMAT_DEFAULT;
      case DEFAULT_DAY_DASH:
        return PrimusConstants.DATE_FORMAT_DASH;
      case DAY_RANGE:
        return PrimusConstants.DATE_FORMAT_RANGE;
      default:
        throw new IOException("Unknown time format " + timeFormat);
    }
  }

  public static List<BaseInput> scanHourInput(FileSystem fileSystem,
      FileSourceInput fileSourceInput, int startDay, int startHour, int endDay, int endHour,
      DayFormat dayFormat, boolean dayKey, boolean checkSuccess)
      throws ParseException, IOException {
    List<BaseInput> results = new LinkedList<>();
    int fileDay = startDay;
    while (fileDay <= endDay) {
      int fileStartHour = (fileDay == startDay ? startHour : 0);
      int fileEndHour = (fileDay == endDay ? endHour : 23);
      for (int fileHour = fileStartHour; fileHour <= fileEndHour; fileHour++) {
        String hourPath = getHourPath(fileHour);
        String inputKey = dayKey ? String.valueOf(fileDay) : fileDay + hourPath;
        String dayPath =
            fileSourceInput.getSpec().getPathPattern() + "/" + getDayPath(fileDay, dayFormat) + "/";
        String inputPath;
        Path successFilePath;
        if (fileSystem.isDirectory(new Path(dayPath + hourPath))) {
          inputPath = dayPath + hourPath + "/" + fileSourceInput.getSpec().getNamePattern();
          successFilePath = new Path(dayPath + hourPath + "/_SUCCESS");
        } else {
          inputPath = dayPath + hourPath + fileSourceInput.getSpec().getNamePattern();
          successFilePath = new Path(dayPath + "_" + hourPath + "_SUCCESS");
        }
        if (checkSuccess && !isFileExist(fileSystem, successFilePath)) {
          return null;
        }
        results.add(new PrimusInput(
            fileSourceInput.getSourceId(),
            fileSourceInput.getSource(),
            inputKey,
            inputPath,
            fileSourceInput.getSpec()));
      }
      fileDay = TimeUtils.plusDay(fileDay, 1);
    }
    return results;
  }

  public static String getHourPath(int hour) {
    return String.format("%02d", hour);
  }

  public static URI resolveURI(String file) throws URISyntaxException {
    URI uri = new URI(file);
    if (uri.getScheme() != null) {
      return uri;
    }
    if (uri.getFragment() != null) {
      URI absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI();
      return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment());
    }
    return new File(file).getAbsoluteFile().toURI();
  }

  public static Path getQualifiedLocalPath(URI localURI, Configuration hadoopConf)
      throws IOException, URISyntaxException {
    URI qualifiedURI = localURI;
    if (localURI.getScheme() == null) {
      qualifiedURI =
          new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString());
    }
    return new Path(qualifiedURI);
  }

  public static void addResource(
      FileSystem fs,
      Path path,
      String linkname,
      Map<String, LocalResource> cacheFiles
  ) throws IOException {
    LocalResource rsrc = new LocalResourcePBImpl();
    FileStatus rsrcStat = fs.getFileStatus(path);
    URL resource = ConverterUtils.getYarnUrlFromPath(fs.resolvePath(rsrcStat.getPath()));
    LOG.info("Add resource[" + resource + "], path[" + path + "], linkname[" + linkname + "]");
    rsrc.setResource(resource);
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(LocalResourceType.FILE);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    cacheFiles.put(linkname, rsrc);
  }

  public static String buildLinkname(Path path, URI localURI, String destName, String targetDir) {
    String linkname;
    linkname = "";
    if (targetDir != null) {
      linkname += targetDir + "/";
    }
    if (destName != null) {
      linkname += destName;
    } else if (localURI.getFragment() != null) {
      linkname += localURI.getFragment();
    } else {
      linkname += path.getName();
    }

    if (linkname.isEmpty()) {
      linkname = path.getParent().getName();
    }

    return linkname;
  }

  public static Boolean addDistributedUri(URI uri,
      Set<String> distributedUris,
      Set<String> distributedNames) {
    String uriStr = uri.toString();
    String fileName = new File(uri.getPath()).getName();
    if (distributedUris.contains(uriStr)) {
      LOG.warn("Same path resource " + uri + " added multiple times to distributed cache.");
      return false;
    } else if (distributedNames.contains(fileName)) {
      LOG.warn("Same name resource " + uri + " added multiple times to distributed cache");
      return false;
    } else {
      distributedUris.add(uriStr);
      distributedNames.add(fileName);
      return true;
    }
  }
}