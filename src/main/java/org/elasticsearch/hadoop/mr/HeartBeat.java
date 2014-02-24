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
    private final TimeValue rate;
    private final Log log;
    private final String id;

    HeartBeat(final Progressable progressable, Configuration cfg, TimeValue delay, final Log log) {
        Assert.notNull(progressable, "a valid progressable is required to report status to Hadoop");
        TimeValue tv = HadoopCfgUtils.getTaskTimeout(cfg);
        Assert.isTrue(tv.getSeconds() > delay.getSeconds(), "Hadoop timeout is shorter than the heartbeat");

        this.progressable = progressable;
        this.rate = new TimeValue(tv.getMillis() - delay.getMillis(), TimeUnit.MILLISECONDS);
        this.log = log;
        TaskID taskID = TaskID.forName(HadoopCfgUtils.getTaskId(cfg));

        if (taskID == null) {
            log.error(String.format("Cannot determine task id - current properties are %s", HadoopCfgUtils.asProperties(cfg)));
        }

        Assert.notNull(taskID,
                "Unable to determine task id - please report your distro/setting through the issue tracker");

        this.id = taskID.toString();
    }


    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        if (log != null && log.isTraceEnabled()) {
            log.trace(String.format("Starting heartbeat for %s", id));
        }

        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                if (log != null && log.isTraceEnabled()) {
                    log.trace(String.format("Heartbeat/progress sent to Hadoop for %s", id));
                }
                progressable.progress();
            }
            // start the reporter before timing out
        }, rate.getMillis(), TimeUnit.MILLISECONDS);
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
