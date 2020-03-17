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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

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

    static PigWrapper pig;

    @BeforeClass
    public static void startup() throws Exception {
        pig = new PigWrapper(PigSuite.tempFolder);
        pig.start();
    }

    @AfterClass
    public static void shutdown() {
        pig.stop();
    }

    public static String getResults(String outputFolder) {
        File fl = new File(outputFolder);
        assertThat(String.format("Folder [%s] not found", outputFolder), fl.exists(), is(true));
        assertThat(new File(outputFolder, "_SUCCESS").exists(), is(true));

        File[] files = fl.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return (name.startsWith("part-r-") || name.startsWith("part-m-"));
            }
        });

        StringBuilder content = new StringBuilder();

        for (File file : files) {
            try {
                String data = IOUtils.asString(new FileInputStream(file)).trim();
                if (StringUtils.hasText(data)) {
                    content.append(data);
                    content.append("\n");
                }
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        return content.toString();
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
