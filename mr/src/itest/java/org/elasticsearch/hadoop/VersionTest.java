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
package org.elasticsearch.hadoop;

import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VersionTest {

    @Test
    public void testNormalize() throws Exception {

        List<URL> urls = new ArrayList<URL>();
        urls.add(new URL("jar:file:/tmp/mesos/slaves/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/frameworks/3014350f-cd05-44af-9c9c-3974bdeed86e-0016/executors/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/runs/3697bc14-f0bd-48b9-8599-9683867eec63/elasticsearch-spark_2.10-2.2.0-m1.jar!/"));
        urls.add(new URL("jar:file:/tmp/mesos/slaves/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/frameworks/3014350f-cd05-44af-9c9c-3974bdeed86e-0016/executors/3014350f-cd05-44af-9c9c-3974bdeed86e-S1/runs/3697bc14-f0bd-48b9-8599-9683867eec63/./elasticsearch-spark_2.10-2.2.0-m1.jar!/"));
        String originPath = url.get(0).toString();
        File file = new File(originPath);
        if (file.createNewFile()){
          System.out.println("File is created!");
        } else {
          System.out.println("File already exists.");
        }
        Path existingFilePath = Paths.get(originPath);
        Path symLinkPath = Paths.get(System.getProperty("java.io.tmpdir") + File.separator + "elasticsearch-spark_2.10-2.2.0-m1.jar");
        Files.createSymbolicLink(symLinkPath, existingFilePath);

        Set<String> normalized = StringUtils.normalize(urls);
        System.out.println(normalized);
        assertEquals(1, normalized.size());
    }
}
