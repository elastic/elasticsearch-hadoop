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
package org.elasticsearch.hadoop.integration.hive;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

public class ThriftHiveInstance implements HiveInstance {

    private static Log log = LogFactory.getLog(ThriftHiveInstance.class);

    private HiveClient client;

    private String host;
    private int port;

    public ThriftHiveInstance(String hiveHost) {
        int split = hiveHost.indexOf(":");
        if (split < 0) {
            host = hiveHost;
            port = 10000;
        }
        else {
            host = hiveHost.substring(0, split);
            port = Integer.valueOf(hiveHost.substring(split + 1));
        }
    }

    @Override
    public void start() throws Exception {
        log.info(String.format("Starting Hive Thrift/Client on [%s][%d]...", host, port));

        TSocket transport = new TSocket(host, port, (int) TimeUnit.MINUTES.toMillis(2));
        client = new HiveClient(new TBinaryProtocol(transport));
        transport.open();
    }

    @Override
    public List<String> execute(String statement) throws Exception {
        client.execute(statement);
        return client.fetchAll();
    }

    @Override
    public void stop() throws Exception {
        if (client != null) {
            log.info(String.format("Stopping Hive Thrift/Client on [%s][%d]...", host, port));
            client.clean();
            client.shutdown();
            client = null;
        }
    }
}
