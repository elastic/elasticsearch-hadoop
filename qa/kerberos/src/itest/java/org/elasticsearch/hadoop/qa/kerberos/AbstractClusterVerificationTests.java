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

package org.elasticsearch.hadoop.qa.kerberos;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AbstractClusterVerificationTests {

    @Parameters
    public static Collection<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{"mr",        "part-m-", 345, true});
        params.add(new Object[]{"sparkRDD",  "part-",   345, true});
        params.add(new Object[]{"sparkDF",   "part-",   345, true});
        params.add(new Object[]{"sparkDS",   "part-",   345, true});
        params.add(new Object[]{"hive",      "000000_0",     345, false});
        params.add(new Object[]{"pig",       "part-m-", 345, true});
        return params;
    }

    private String integrationName;
    private String dataFilePrefix;
    private int expectedLineCount;
    private boolean hasSuccessMarker;

    public AbstractClusterVerificationTests(String integrationName, String dataFilePrefix, int expectedLineCount, boolean hasSuccessMarker) {
        this.integrationName = integrationName;
        this.dataFilePrefix = dataFilePrefix;
        this.expectedLineCount = expectedLineCount;
        this.hasSuccessMarker = hasSuccessMarker;
    }

    @Test
    public void testOutputContents() throws Exception {
        File dataDirectory = new File("build/data");
        File integrationDataDir = new File(dataDirectory, integrationName);

        if (hasSuccessMarker) {
            File successMarker = new File(integrationDataDir, "_SUCCESS");
            Assert.assertTrue("Success marker expected for integration [" + integrationName + "] but could not find it.",
                    successMarker.exists());
        }

        int lines = 0;
        for (File directoryFile : integrationDataDir.listFiles()) {
            if (directoryFile.getName().startsWith(dataFilePrefix)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(directoryFile)));
                while (reader.readLine() != null) {
                    lines++;
                }
                reader.close();
            }
        }
        Assert.assertEquals(expectedLineCount, lines);
    }
}
