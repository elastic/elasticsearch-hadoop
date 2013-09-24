/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.integration.pig;

import java.io.ByteArrayInputStream;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;

/**
 * Wrapper around Pig.
 */
public class PigWrapper {

    private PigServer pig;

    public void start() {
        try {
            pig = createPig();
        } catch (ExecException ex) {
            throw new IllegalStateException("Cannot create pig server", ex);
        }
        pig.setBatchOn();
    }

    protected PigServer createPig() throws ExecException {
        HdpBootstrap.hackHadoopStagingOnWin();
        Properties properties = new TestSettings().getProperties();
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
        pig.executeBatch();
        pig.discardBatch();
        pig.setBatchOn();
    }
}
