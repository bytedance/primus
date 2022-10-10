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

import static com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputType.RAW_INPUT;
import static com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputType.TEXT_INPUT;

import com.bytedance.primus.am.datastream.file.FileSourceInput;
import com.bytedance.primus.am.datastream.file.PrimusInput;
import com.bytedance.primus.am.datastream.file.PrimusSplit;
import com.bytedance.primus.am.datastream.file.operator.Input;
import com.bytedance.primus.apiserver.proto.DataProto;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.InputType;
import com.bytedance.primus.proto.PrimusCommon.DayFormat;
import com.bytedance.primus.proto.PrimusConfOuterClass.PrimusConf;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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

  public static InputType getInputType(Path path, FileSystem fs)
      throws IllegalArgumentException, IOException {
    FileStatus[] fss;
    if ((fss = fs.globStatus(path)).length > 0) {
      for (FileStatus globStat : fss) {
        FileStatus[] fileStatuses = fs.listStatus(new Path(globStat.getPath().toUri().getPath()));
        for (FileStatus fileStatus : fileStatuses) {
          if (isIgnoredFile(fileStatus.getPath())) {
            continue;
          }
          InputType inputType = getInputType(fileStatus.getPath().getName());
          LOG.info("FileType compute:{}, path:{}, file path:{}", inputType, path,
              fileStatus.getPath().getName());
          return inputType;
        }
      }
    }
    if (fs.isDirectory(path)) {
      FileStatus[] fileStatuses = fs.listStatus(path);
      for (FileStatus fileStatus : fileStatuses) {
        if (isIgnoredFile(fileStatus.getPath())) {
          continue;
        }
        InputType inputType = getInputType(fileStatus.getPath().getName());
        LOG.info("FileType compute:{}, path:{}, file path:{}", inputType, path,
            fileStatus.getPath().getName());
        return inputType;
      }
    }
    InputType inputType = getInputType(path.getName());
    LOG.info("FileType compute:{}, path:{}, name:{}", inputType, path, path.getName());
    return inputType;
  }

  public static boolean isSplittable(InputType inputType) {
    return false;
  }

  public static boolean isIgnoredFile(Path path) {
    return path.getName().endsWith("_SUCCESS")
        || path.getName().equals("_temporary")
        || path.toUri().getPath().contains("/_temporary/");
  }

  public static InputType getInputType(String path) {
    if (path.contains(".txt")) {
      return TEXT_INPUT;
    } else {
      return RAW_INPUT;
    }
  }

  public static SortedSet<PrimusSplit> scanPattern(PrimusInput input, FileSystem fs,
      Configuration conf) throws IllegalArgumentException, IOException {
    SortedSet<PrimusSplit> ret = new TreeSet<>();
    switch (input.getInputType()) {
      case RAW_INPUT:
      case TEXT_INPUT: {
        FileStatus[] matches = fs.globStatus(new Path(input.getPath()));
        if (matches == null) {
          throw new NoSuchFileException("Input path does not exist: " + input.getPath());
        } else if (matches.length == 0) {
          throw new NoSuchFileException("Input Pattern " + input.getPath() + " matches 0 files");
        }
        for (FileStatus globStat : matches) {
          FileStatus[] fileStatuses;
          if (globStat.isDirectory()) {
            // scan sub-directory if current globStat directory is day+hour path (YYYYMMDD/HH)
            fileStatuses = fs.listStatus(new Path(globStat.getPath().toUri().getPath()));
          } else {
            fileStatuses = new FileStatus[]{globStat};
          }
          for (FileStatus fileStatus : fileStatuses) {
            Path pathForFilter = new Path(globStat.getPath(), fileStatus.getPath());
            if (isIgnoredFile(pathForFilter)) {
              continue;
            }
            PrimusSplit split = new PrimusSplit(
                input.getSourceId(),
                input.getSource(),
                input.getKey(),
                fileStatus.getPath().toString(),
                0, fileStatus.getLen(),
                input.getInputType());
            ret.add(split);
          }
        }
        break;
      }
      default:
        throw new IllegalArgumentException("Unsupported input type: " + input.getInputType());
    }
    return ret;
  }

  public static Message buildMessageFromJson(String jsonPath, Message.Builder builder)
      throws IOException {
    InputStream in = new FileInputStream(jsonPath);
    Reader reader = new InputStreamReader(in);
    JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
    parser.merge(reader, builder);
    return builder.build();
  }

  public static PrimusConf buildPrimusConf(String configPath) throws IOException {
    try {
      return (PrimusConf) buildMessageFromJson(configPath, PrimusConf.newBuilder());
    } catch (IOException e) {
      throw new IOException("Config parse failed", e);
    }
  }

  public static DataProto.DataSpec buildDataSpec(String dataSpecPath)
      throws IOException {
    return (DataProto.DataSpec) buildMessageFromJson(dataSpecPath,
        DataProto.DataSpec.newBuilder());
  }

  private static boolean isFileExist(FileSystem fileSystem, Path path) throws IOException {
    boolean result = fileSystem.exists(path);
    LOG.info("Check file[" + path + "] exist: " + result);
    return result;
  }

  public static List<Input> scanDayInput(FileSystem fileSystem,
      FileSourceInput fileSourceInput, int startDay, int endDay, DayFormat timeFormat,
      boolean checkSuccess) throws ParseException, IOException {

    List<Input> results = new LinkedList<>();
    for (
        int fileDay = startDay;
        fileDay <= endDay;
        fileDay = Integer.valueOf(TimeUtils.plusDay(fileDay, 1))
    ) {
      String dayPath = fileSourceInput.getInput() + "/" + getDayPath(fileDay, timeFormat) + "/";
      Path successFilePath = new Path(dayPath + "_SUCCESS");
      String inputPath = dayPath + fileSourceInput.getFileNameFilter();
      if (checkSuccess && !isFileExist(fileSystem, successFilePath)) {
        return null;
      }
      PrimusInput input = new PrimusInput(
          fileSourceInput.getSourceId(),
          fileSourceInput.getSource(),
          String.valueOf(fileDay),
          inputPath,
          fileSourceInput.getInputType());
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

  public static List<Input> scanHourInput(FileSystem fileSystem,
      FileSourceInput fileSourceInput, int startDay, int startHour, int endDay, int endHour,
      DayFormat dayFormat, boolean dayKey, boolean checkSuccess, Configuration conf)
      throws ParseException, IOException {
    List<Input> results = new LinkedList<>();
    int fileDay = startDay;
    while (fileDay <= endDay) {
      int fileStartHour = (fileDay == startDay ? startHour : 0);
      int fileEndHour = (fileDay == endDay ? endHour : 23);
      for (int fileHour = fileStartHour; fileHour <= fileEndHour; fileHour++) {
        String hourPath = getHourPath(fileHour);
        String inputKey = dayKey ? String.valueOf(fileDay) : fileDay + hourPath;
        String dayPath =
            fileSourceInput.getInput() + "/" + getDayPath(fileDay, dayFormat) + "/";
        String inputPath;
        Path successFilePath;
        if (fileSystem.isDirectory(new Path(dayPath + hourPath))) {
          inputPath = dayPath + hourPath + "/" + fileSourceInput.getFileNameFilter();
          successFilePath = new Path(dayPath + hourPath + "/_SUCCESS");
        } else {
          inputPath = dayPath + hourPath + fileSourceInput.getFileNameFilter();
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
            fileSourceInput.getInputType()));
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
      Path path,
      String linkname,
      Configuration conf,
      Map<String, LocalResource> cacheFiles
  ) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), conf);
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