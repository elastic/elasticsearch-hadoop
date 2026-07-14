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
package org.elasticsearch.hadoop.hive;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.junit.Test;

import static org.elasticsearch.hadoop.hive.HiveConstants.TABLE_LOCATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EsStorageHandlerTest {

    /**
     * The write path stores the committer under the old-API key {@code mapred.output.committer.class}, which engines
     * such as Tez instantiate and cast to {@link org.apache.hadoop.mapred.OutputCommitter} during vertex init. The
     * configured class must therefore be an old-API committer; configuring the new-API
     * {@link org.apache.hadoop.mapreduce.OutputCommitter} subtype broke Tez (issue #2533).
     */
    @Test
    public void outputCommitterIsOldApiForTezCompatibility() throws Exception {
        EsStorageHandler handler = new EsStorageHandler();
        handler.setConf(new Configuration());

        Properties props = new Properties();
        props.setProperty(TABLE_LOCATION, "some-table");
        TableDesc tableDesc = new TableDesc();
        tableDesc.setProperties(props);

        Map<String, String> jobProperties = new HashMap<String, String>();
        handler.configureOutputJobProperties(tableDesc, jobProperties);

        String committer = HadoopCfgUtils.getOutputCommitterClass(handler.getConf());
        assertEquals(EsOutputFormat.EsOldAPIOutputCommitter.class.getName(), committer);
        assertTrue("committer must be an old-API org.apache.hadoop.mapred.OutputCommitter",
                org.apache.hadoop.mapred.OutputCommitter.class.isAssignableFrom(Class.forName(committer)));
    }
}
