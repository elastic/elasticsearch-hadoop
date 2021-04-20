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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import static org.hamcrest.Matchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractPigTests {

    static Configuration testConfiguration;
    static PigWrapper pig;

    @BeforeClass
    public static void startup() throws Exception {
        testConfiguration = HdpBootstrap.hadoopConfig();
        pig = new PigWrapper(PigSuite.tempFolder);
        pig.start();
    }

    @AfterClass
    public static void shutdown() {
        pig.stop();
    }

    public static String getResults(String outputFolder) {
        Path fl = new Path(outputFolder);
        try {
            FileSystem fileSystem = FileSystem.get(testConfiguration);
            assertThat(String.format("Folder [%s] not found", outputFolder), fileSystem.exists(fl), is(true));
            assertThat(fileSystem.exists(new Path(outputFolder, "_SUCCESS")), is(true));

            FileStatus[] files = fileSystem.listStatus(fl, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return (path.getName().startsWith("part-r-") || path.getName().startsWith("part-m-"));
                }
            });

            StringBuilder content = new StringBuilder();

            for (FileStatus file : files) {
                String data = IOUtils.asString(fileSystem.open(file.getPath())).trim();
                if (StringUtils.hasText(data)) {
                    content.append(data);
                    content.append("\n");
                }
            }

            return content.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String tabify(String...strings) {
        StringBuilder sb = new StringBuilder();
        for (String string : strings) {
            sb.append(string);
            sb.append("\t");
        }

        return sb.substring(0, sb.length() - 1).toString();
    }
}
