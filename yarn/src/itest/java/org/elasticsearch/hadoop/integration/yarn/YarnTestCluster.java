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
package org.elasticsearch.hadoop.integration.yarn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.rules.ExternalResource;

public class YarnTestCluster extends ExternalResource {

    private MiniYARNCluster yarnCluster = null;
    private MiniDFSCluster dfsCluster = null;

    private int nodes = 1;
    private final String CLUSTER_NAME = "es-yarn-cluster";

    private String tempDir;

    private final YarnConfiguration cfg;

    YarnTestCluster(YarnConfiguration cfg) {
        this(cfg, 1);
    }

    YarnTestCluster(YarnConfiguration cfg, int nodes) {
        this.cfg = cfg;
        this.nodes = nodes;
    }

    MiniYARNCluster yarnCluster() {
        return yarnCluster;
    }

    DistributedFileSystem fs() {
        try {
            return dfsCluster.getFileSystem();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    protected void before() throws Throwable {
        if (yarnCluster != null) {
            return;
        }

        System.out.println("Starting YARN cluster...");

        //tempDir = System.getProperty("java.io.tmpdir");
        //System.setProperty("java.io.tmpdir", "/tmp-yarn");

        //        FileContext.DEFAULT_PERM.fromShort((short) 777);
        //        FileContext.DIR_DEFAULT_PERM.fromShort((short) 777);
        //        FileContext.FILE_DEFAULT_PERM.fromShort((short) 777);
        //ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION.fromShort((short) 777);

        // make sure to use IP discovery
        Configuration.addDefaultResource("yarn-test-site.xml");

        // disable am deletion
        cfg.set(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, "-1");
        cfg.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "build/es-yarn/" + CLUSTER_NAME + "-dfs");

        dfsCluster = new MiniDFSCluster.Builder(cfg).numDataNodes(nodes).build();
        System.out.println("Started DFS cluster...");

        cfg.set(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, "true");
        yarnCluster = new MiniYARNCluster(CLUSTER_NAME, nodes, 1, 1);
        yarnCluster.init(cfg);
        yarnCluster.start();
        System.out.println("Started YARN cluster...");
    }

    @Override
    protected void after() {
        //System.setProperty("java.io.tmpdir", tempDir);

        if (yarnCluster != null) {
            System.out.println("Stopping YARN cluster...");
            // same as close but without the IOException
            yarnCluster.stop();
            yarnCluster = null;
        }

        if (dfsCluster != null) {
            System.out.println("Stopping DFS cluster...");
            dfsCluster.shutdown();
            dfsCluster = null;
        }
    }
}