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

package com.bytedance.primus.webapp;

import com.bytedance.primus.webapp.bundles.SummaryBundle;
import com.google.inject.Inject;
import java.text.SimpleDateFormat;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

/**
 * Render all of the jobs that the history server is aware of.
 */
public class HsJobsBlock extends HtmlBlock {

  private final HistoryContext context;
  private final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");

  @Inject
  HsJobsBlock(HistoryContext ctx) {
    context = ctx;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
   */
  @Override
  protected void render(Block html) {
    TBODY<TABLE<Hamlet>> tbody = html.
        h2("Recent Training").
        table("#jobs").
        thead().
        tr().
        th("Start Time").
        th("Finish Time").
        th(".id", "Application ID").
        th(".name", "Name").
        th(".user", "User").
        th(".queue", "Queue").
        th(".attempts", "Attempts").
        th(".state", "State").__().__().tbody();

    LOG.info("Getting list of all Jobs.");
    // Write all the data into a JavaScript array of arrays for JQuery
    // DataTables to display
    StringBuilder jobsTableData = new StringBuilder("[\n");
    for (HistorySnapshot s : context.listRecentApps(200)) {
      SummaryBundle j = s.getStatus().getSummary();
      String appUrl = j.getAttemptId() != 0 ?
          url("app", j.getApplicationId(), String.valueOf(j.getAttemptId()), "") :
          url("app", j.getApplicationId(), "");
      jobsTableData.append("[\"")
          .append(dateFormat.format(j.getStartTime())).append("\",\"")
          .append((j.getFinishTime() != null) ? dateFormat.format(j.getFinishTime()) : "N/A")
          .append("\",\"")
          .append("<a href='")
          .append(appUrl)
          .append("'>")
          .append(j.getApplicationId()).append("</a>\",\"")
          .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
              j.getName()))).append("\",\"")
          .append(j.getUser()).append("\",\"")
          .append(j.getQueue()).append("\",\"")
          .append(j.getAttemptId() != 0 ? j.getAttemptId() : 1).append("\",\"")
          .append((j.getFinalStatus() != null) ? j.getFinalStatus() : "IN_PROGRESS").append("\",\"")
          .append("\"],\n");
    }

    // Remove the last comma and close off the array of arrays
    if (jobsTableData.charAt(jobsTableData.length() - 2) == ',') {
      jobsTableData.delete(jobsTableData.length() - 2, jobsTableData.length() - 1);
    }
    jobsTableData.append("]");
    html.script().$type("text/javascript").
        __("var jobsTableData=" + jobsTableData).
        __();
    tbody.__().
        tfoot().
        tr().
        th().input("search_init").$type(InputType.text).$name("start_time").$value("Start Time")
        .__().__().
        th().input("search__init").$type(InputType.text).$name("finish__time").$value("Finish Time")
        .__().__().
        th().input("search__init").$type(InputType.text).$name("start__time")
        .$value("Application ID")
        .__().__().
        th().input("search__init").$type(InputType.text).$name("start__time").$value("Name")
        .__().__().
        th().input("search__init").$type(InputType.text).$name("start__time").$value("User")
        .__().__().
        th().input("search__init").$type(InputType.text).$name("start__time").$value("Queue")
        .__().__().
        th().input("search__init").$type(InputType.text).$name("start__time").$value("Attempts")
        .__().__().
        th().input("search__init").$type(InputType.text).$name("start__time").$value("State").__()
        .__().
        __().
        __().
        __();
  }
}
