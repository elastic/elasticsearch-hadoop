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
package org.elasticsearch.hadoop.integration.pig;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.integration.QueryTestParams;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Wrapper around Pig.
 */
public class PigWrapper {

    private PigServer pig;

    public void start() {
        try {
            pig = createPig();
        } catch (ExecException ex) {
            throw new EsHadoopIllegalStateException("Cannot create pig server", ex);
        }
        pig.setBatchOn();
    }

    protected PigServer createPig() throws ExecException {
        HdpBootstrap.hackHadoopStagingOnWin();

        Properties properties = HdpBootstrap.asProperties(QueryTestParams.provisionQueries(HdpBootstrap.hadoopConfig()));
        String pigHost = properties.getProperty("pig");
        // remote Pig instance
        if (StringUtils.hasText(pigHost) && !"local".equals(pig)) {
            LogFactory.getLog(PigWrapper.class).info("Executing Pig in Map/Reduce mode");
            return new PigServer(ExecType.MAPREDUCE, properties);
        }

        // use local instance
        LogFactory.getLog(PigWrapper.class).info("Executing Pig in local mode");
        properties.put("mapred.job.tracker", "local");
        return new PigServer(ExecType.LOCAL, properties);
    }

    public void stop() {
        // close pig
        if (pig != null) {
            pig.shutdown();
            pig = null;
        }
    }

    public void executeScript(String script) throws Exception {
        pig.registerScript(new ByteArrayInputStream(script.getBytes()));
        List<ExecJob> executeBatch = pig.executeBatch();
        for (ExecJob execJob : executeBatch) {
            if (execJob.getStatus() == ExecJob.JOB_STATUS.FAILED) {
                throw new EsHadoopIllegalStateException("Pig execution failed");
            }
        }
        pig.discardBatch();
        pig.setBatchOn();

    }
}
