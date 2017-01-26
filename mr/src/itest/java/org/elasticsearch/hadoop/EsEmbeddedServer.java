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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.StringUtils.IpAndPort;

public class EsEmbeddedServer {

    private static final String ES_PORTS_FILE_LOCATION = "es.test.ports.file.location";

    private IpAndPort ipAndPort;

    public EsEmbeddedServer() {
        try {
            String path = System.getProperty(ES_PORTS_FILE_LOCATION);
            if (path == null) {
                // No local ES stood up. Better throw...
                throw new IllegalStateException("Could not find Elasticsearch ports file. Should " +
                        "you be running tests with an external cluster?");
            }

            File portsFile = new File(path);
            try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(portsFile)))) {
                for (String line = in.readLine(); line != null; line = in.readLine()) {
                    if (line.contains("[") || line.contains("]") || line.isEmpty()) {
                        continue;
                    }
                    ipAndPort = StringUtils.parseIpAddress(line);
                    break;
                }
            }
        } catch (Exception e) {
            throw new EsHadoopException("Encountered exception during embedded node startup", e);
        }
    }

    public IpAndPort getIpAndPort() {
        return ipAndPort;
    }
}
