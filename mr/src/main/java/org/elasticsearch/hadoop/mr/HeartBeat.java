/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.mr;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Utility class acting as a heart-beat to Hadoop to prevent it from killing es-hadoop jobs, especially when indexing data which can take time a long time.
 */
class HeartBeat {

    private ScheduledExecutorService scheduler;
    private final Progressable progressable;
    private final TimeValue delay;
    private final Log log;
    private final String id;

    HeartBeat(final Progressable progressable, Configuration cfg, TimeValue lead, final Log log) {
        Assert.notNull(progressable, "a valid progressable is required to report status to Hadoop");
        TimeValue tv = HadoopCfgUtils.getTaskTimeout(cfg);

        Assert.isTrue(tv.getSeconds() <= 0 || tv.getSeconds() > lead.getSeconds(), "Hadoop timeout is shorter than the heartbeat");

        this.progressable = progressable;
        long cfgMillis = (tv.getMillis() > 0 ? tv.getMillis() : 0);
        // the task is simple hence the delay = timeout - lead, that is when to start the notification right before the timeout
        this.delay = new TimeValue(Math.abs(cfgMillis - lead.getMillis()), TimeUnit.MILLISECONDS);
        this.log = log;

        String taskId;
        TaskID taskID = HadoopCfgUtils.getTaskID(cfg);

        if (taskID == null) {
            log.warn("Cannot determine task id...");
            taskId = "<unknown>";
            if (log.isTraceEnabled()) {
                log.trace("Current configuration is " + HadoopCfgUtils.asProperties(cfg));
            }
        }
        else {
            taskId = "" + taskID;
        }

        id = taskId;
    }


    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        if (log != null && log.isTraceEnabled()) {
            log.trace(String.format("Starting heartbeat for %s", id));
        }

        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (log != null && log.isTraceEnabled()) {
                    log.trace(String.format("Heartbeat/progress sent to Hadoop for %s", id));
                }
                progressable.progress();
            }
            // start the reporter before timing out
        }, delay.getMillis(), delay.getMillis(), TimeUnit.MILLISECONDS);
    }

    void stop() {
        if (log != null && log.isTraceEnabled()) {
            log.trace(String.format("Stopping heartbeat for %s", id));
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
