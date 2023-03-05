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

package com.bytedance.primus.am.datastream.file.task.builder;

import static com.bytedance.primus.utils.TimeUtils.newDate;
import static com.bytedance.primus.utils.TimeUtils.newDateHour;
import static com.bytedance.primus.utils.TimeUtils.newNow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec;
import com.bytedance.primus.apiserver.proto.DataProto.FileSourceSpec.RawInput;
import com.bytedance.primus.proto.PrimusCommon.Time;
import com.bytedance.primus.proto.PrimusCommon.TimeRange;
import com.bytedance.primus.utils.TimeUtils;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestTimeRangeIterator {

  private FileSourceInput newInput(String key, Time start, Time end) {
    return new FileSourceInput(
        0,        // sourceID
        "source", // source
        FileSourceSpec.newBuilder()
            .setPathPattern(key)
            .setNamePattern(key)
            .setTimeRange(TimeRange.newBuilder()
                .setFrom(start)
                .setTo(end))
            .setRawInput(RawInput.getDefaultInstance())
            .build());
  }

  private List<FileSourceInput> newFileSourceInputList(Time... times) {
    List<FileSourceInput> ret = new LinkedList<>();
    for (int i = 0; i < times.length; i += 2) {
      ret.add(newInput("KEY", times[i], times[i + 1]));
    }
    return ret;
  }

  private void assertBooleanEquals(boolean result, boolean expectation) {
    assertEquals(result, expectation);
  }

  private void checkIteratorStates(
      TimeRangeIterator iter,
      boolean isValid,
      String batchKey,
      List<FileSourceInput> inputs
  ) {
    assertBooleanEquals(isValid, iter.isValid());
    assertEquals(batchKey, iter.getBatchKey());
    assertEquals(inputs, iter.peekNextBatch());
  }

  @Test
  public void testEmptyInput() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(new LinkedList<>());

    checkIteratorStates(
        iter,
        false, // isValid
        null,  // batch key
        new LinkedList<>()
    );

    // 1st iteration
    assertBooleanEquals(iter.prepareNextBatch(), false);
  }

  @Test
  public void testSingleClosedInputWithDate() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newDate(20201231), newDate(20210101)
        )
    );

    checkIteratorStates(
        iter,
        true,       // isValid
        "20201230", // batch key
        new LinkedList<>()
    );

    // 1st
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,       // isValid
        "20201231", // batch key
        newFileSourceInputList(newDate(20201231), newDate(20201231))
    );

    assertBooleanEquals(iter.prepareNextBatch(), true); // Prepare without pop, nothing changes.
    checkIteratorStates(
        iter,
        true,       // isValid
        "20201231", // batch key
        newFileSourceInputList(newDate(20201231), newDate(20201231))
    );

    iter.popNextBatch(); // The batch is popped, while other fields remain.
    checkIteratorStates(
        iter,
        true,       // isValid
        "20201231", // batch key
        new LinkedList<>()
    );

    // 2nd
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,       // isValid
        "20210101", // batch key
        newFileSourceInputList(newDate(20210101), newDate(20210101))
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,       // isValid
        "20210101", // batch key
        new LinkedList<>()
    );

    // 3rd
    assertBooleanEquals(iter.prepareNextBatch(), false);
    checkIteratorStates(
        iter,
        false,      // isValid
        "20210102", // batch key
        new LinkedList<>()
    );
  }

  @Test
  public void testSingleOpenInputWithDate() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(newDate(20200101), newNow())
    );

    checkIteratorStates(
        iter,
        true,       // isValid
        "20191231", // batch key
        new LinkedList<>()
    );

    // Prepare
    for (int batchKey : new int[]{
        20200101, 20200102, 20200103, 20200104, 20200105,
        20200106, 20200107, 20200108, 20200109, 20200110,
        20200111, 20200112, 20200113, 20200114, 20200115,
        20200116, 20200117, 20200118, 20200119, 20200120,
        20200121, 20200122, 20200123, 20200124, 20200125,
        20200126, 20200127, 20200128, 20200129, 20200130,
        20200131, 20200201, 20200202, 20200203, 20200204,
    }) {
      // Prepare
      assertBooleanEquals(iter.prepareNextBatch(), true);
      checkIteratorStates(
          iter,
          true,                     // isValid
          String.valueOf(batchKey), // batch cursor
          newFileSourceInputList(newDate(batchKey), newDate(batchKey))
      );

      // Pop
      iter.popNextBatch();
      checkIteratorStates(
          iter,
          true,                     // isValid
          String.valueOf(batchKey), // batch cursor
          new LinkedList<>()
      );
    }
  }

  @Test
  public void testMultipleInputsWithDate() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(
        new LinkedList<FileSourceInput>() {{
          add(newInput("A", newDate(20200101), newDate(20200102)));
          add(newInput("B", newDate(20200101), newDate(20200102))); // Overlapped
          add(newInput("C", newDate(20200103), newDate(20200104))); // Jointed
          add(newInput("D", newDate(20200106), newDate(20200106))); // Disjointed
        }});

    checkIteratorStates(
        iter,
        true,              // isValid
        "20191231", // batch key
        new LinkedList<>()
    );

    // 1st
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200101", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("A", newDate(20200101), newDate(20200101)));
          add(newInput("B", newDate(20200101), newDate(20200101)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200101", // batch key
        new LinkedList<>()
    );

    // 2nd
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200102", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("A", newDate(20200102), newDate(20200102)));
          add(newInput("B", newDate(20200102), newDate(20200102)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200102", // batch key
        new LinkedList<>()
    );

    // 3rd
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200103", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("C", newDate(20200103), newDate(20200103)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200103", // batch key
        new LinkedList<>()
    );

    // 4th
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200104", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("C", newDate(20200104), newDate(20200104)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200104", // batch key
        new LinkedList<>()
    );

    // 5th
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200105", // batch key
        new LinkedList<>()
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200105", // batch key
        new LinkedList<>()
    );

    // 6th
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200106", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("D", newDate(20200106), newDate(20200106)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,              // isValid
        "20200106", // batch key
        new LinkedList<>()
    );

    // 7th
    assertBooleanEquals(iter.prepareNextBatch(), false);
    checkIteratorStates(
        iter,
        false,             // isValid
        "20200107", // batch key
        new LinkedList<>()
    );
  }

  @Test
  public void testSingleClosedInputWithDateHour() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(
            newDateHour(20201231, 23), newDateHour(20210101, 0)
        )
    );

    checkIteratorStates(
        iter,
        true,         // isValid
        "2020123122", // batch key
        new LinkedList<>()
    );

    // 1st
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,         // isValid
        "2020123123", // batch key
        newFileSourceInputList(newDateHour(20201231, 23), newDateHour(20201231, 23))
    );

    assertBooleanEquals(iter.prepareNextBatch(), true); // Prepare without pop, nothing changes.
    checkIteratorStates(
        iter,
        true,         // isValid
        "2020123123", // batch key
        newFileSourceInputList(newDateHour(20201231, 23), newDateHour(20201231, 23))
    );

    iter.popNextBatch(); // The batch is popped, while other fields remain.
    checkIteratorStates(
        iter,
        true,         // isValid
        "2020123123", // batch key
        new LinkedList<>()
    );

    // 2nd
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,        // isValid
        "202101010", // batch key
        newFileSourceInputList(newDateHour(20210101, 0), newDateHour(20210101, 0))
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,        // isValid
        "202101010", // batch key
        new LinkedList<>()
    );

    // 3rd
    assertBooleanEquals(iter.prepareNextBatch(), false);
    checkIteratorStates(
        iter,
        false,       // isValid
        "202101011", // batch key
        new LinkedList<>()
    );
  }

  @Test
  public void testSingleOpenInputWithDateHour() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(
        newFileSourceInputList(newDateHour(20200101, 0), newNow())
    );

    checkIteratorStates(
        iter,
        true,         // isValid
        "2019123123", // batch key
        new LinkedList<>()
    );

    // Prepare
    for (Time cursor : new Time[]{
        TimeUtils.newDateHour(20200101, 0), TimeUtils.newDateHour(20200101, 1),
        TimeUtils.newDateHour(20200101, 2), TimeUtils.newDateHour(20200101, 3),
        TimeUtils.newDateHour(20200101, 4), TimeUtils.newDateHour(20200101, 5),
        TimeUtils.newDateHour(20200101, 6), TimeUtils.newDateHour(20200101, 7),
        TimeUtils.newDateHour(20200101, 8), TimeUtils.newDateHour(20200101, 9),
        TimeUtils.newDateHour(20200101, 10), TimeUtils.newDateHour(20200101, 11),
        TimeUtils.newDateHour(20200101, 12), TimeUtils.newDateHour(20200101, 13),
        TimeUtils.newDateHour(20200101, 14), TimeUtils.newDateHour(20200101, 15),
        TimeUtils.newDateHour(20200101, 16), TimeUtils.newDateHour(20200101, 17),
        TimeUtils.newDateHour(20200101, 18), TimeUtils.newDateHour(20200101, 19),
        TimeUtils.newDateHour(20200101, 20), TimeUtils.newDateHour(20200101, 21),
        TimeUtils.newDateHour(20200101, 22), TimeUtils.newDateHour(20200101, 23),
        TimeUtils.newDateHour(20200102, 0), TimeUtils.newDateHour(20200102, 1),
    }) {
      int date = cursor.getDateHour().getDate();
      int hour = cursor.getDateHour().getHour();

      // Prepare
      assertBooleanEquals(iter.prepareNextBatch(), true);
      checkIteratorStates(
          iter,
          true, // isValid
          String.format("%d%d", date, hour), // batch cursor
          newFileSourceInputList(cursor, cursor)
      );

      // Pop
      iter.popNextBatch();
      checkIteratorStates(
          iter,
          true, // isValid
          String.format("%d%d", date, hour), // batch cursor
          new LinkedList<>()
      );
    }
  }

  @Test
  public void testMultipleInputsWithDateHour() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(
        new LinkedList<FileSourceInput>() {{
          add(newInput("A",
              newDateHour(20200101, 1),
              newDateHour(20200101, 2)));
          add(newInput("B",
              newDateHour(20200101, 1),
              newDateHour(20200101, 2))); // Overlapped
          add(newInput("C",
              newDateHour(20200101, 3),
              newDateHour(20200101, 4))); // Jointed
          add(newInput("D",
              newDateHour(20200101, 6),
              newDateHour(20200101, 6))); // Disjointed
        }});

    checkIteratorStates(
        iter,
        true,        // isValid
        "202001010", // batch key
        new LinkedList<>()
    );

    // 1st
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001011", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("A", newDateHour(20200101, 1), newDateHour(20200101, 1)));
          add(newInput("B", newDateHour(20200101, 1), newDateHour(20200101, 1)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001011", // batch key
        new LinkedList<>()
    );

    // 2nd
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001012", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("A", newDateHour(20200101, 2), newDateHour(20200101, 2)));
          add(newInput("B", newDateHour(20200101, 2), newDateHour(20200101, 2)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001012", // batch key
        new LinkedList<>()
    );

    // 3rd
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001013", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("C", newDateHour(20200101, 3), newDateHour(20200101, 3)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001013", // batch key
        new LinkedList<>()
    );

    // 4th
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001014", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("C", newDateHour(20200101, 4), newDateHour(20200101, 4)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001014", // batch key
        new LinkedList<>()
    );

    // 5th
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001015", // batch key
        new LinkedList<>()
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001015", // batch key
        new LinkedList<>()
    );

    // 6th
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001016", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("D", newDateHour(20200101, 6), newDateHour(20200101, 6)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,        // isValid
        "202001016", // batch key
        new LinkedList<>()
    );

    // 7th
    assertBooleanEquals(iter.prepareNextBatch(), false);
    checkIteratorStates(
        iter,
        false,       // isValid
        "202001017", // batch key
        new LinkedList<>()
    );
  }

  @Test
  public void testMultipleInputsWithMixedTimeCases() throws ParseException {
    // Initialization
    TimeRangeIterator iter = new TimeRangeIterator(
        new LinkedList<FileSourceInput>() {{
          add(newInput("A",
              newDate(20200101),
              newDate(20200101)));
          add(newInput("B",
              newDateHour(20200101, 13),
              newDateHour(20200102, 12)));
        }});

    checkIteratorStates(
        iter,
        true,       // isValid
        "20191231", // batch key
        new LinkedList<>()
    );

    // 1st
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,       // isValid
        "20200101", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("A", newDate(20200101), newDate(20200101)));
          add(newInput("B", newDateHour(20200101, 13), newDateHour(20200101, 23)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,       // isValid
        "20200101", // batch key
        new LinkedList<>()
    );

    // 2nd
    assertBooleanEquals(iter.prepareNextBatch(), true);
    checkIteratorStates(
        iter,
        true,       // isValid
        "20200102", // batch key
        new LinkedList<FileSourceInput>() {{
          add(newInput("B", newDateHour(20200102, 0), newDateHour(20200102, 12)));
        }}
    );

    iter.popNextBatch();
    checkIteratorStates(
        iter,
        true,       // isValid
        "20200102", // batch key
        new LinkedList<>()
    );

    // 3rd
    assertBooleanEquals(iter.prepareNextBatch(), false);
    checkIteratorStates(
        iter,
        false,      // isValid
        "20200103", // batch key
        new LinkedList<>()
    );
  }
}
