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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.elasticsearch.hadoop.yarn.cli.YarnBootstrap;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.elasticsearch.hadoop.integration.yarn.YarnSuite.CFG;
import static org.elasticsearch.hadoop.integration.yarn.YarnSuite.CLIENT_JAR;
import static org.elasticsearch.hadoop.integration.yarn.YarnSuite.YC;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class YarnTest {

    private YarnBootstrap bootstrap;
    private final List<String> testEnv = new ArrayList<String>();

    @Before
    public void before() {
        bootstrap = new YarnBootstrap();
        bootstrap.setConf(CFG);

        testEnv.add("hdfs.es.yarn.jar=" + CLIENT_JAR.getName());
        testEnv.add("internal.es.yarn.file=" + CLIENT_JAR.getAbsolutePath());
        testEnv.add("download.local.dir=./build/downloads");
        // for tests we don't need gigs
        testEnv.add("container.mem=512");
        testEnv.add("sysProp.es.security.manager.enabled=false");
    }

    @Test
    public void testStartup() throws Exception {
        System.out.println(YC.listApps());
    }

    @Test
    public void test1InstallEsYarn() throws Exception {
        bootstrap.run(cmdArgs("-install"));
    }

    @Test
    public void test2Download() throws Exception {
        bootstrap.run(cmdArgs("-download-es"));
    }

    @Test
    public void test3Install() throws Exception {
        bootstrap.run(cmdArgs("-install-es"));
    }

    @Test
    public void test4Start() throws Exception {
        bootstrap.run(cmdArgs("-start", "loadConfig=" + getClass().getResource("/extra.properties").toURI().toString()));
        final List<ApplicationReport> apps = YC.listEsClusters();
        System.out.println(apps);
        final ApplicationId appId = apps.get(0).getApplicationId();
        YC.waitForApp(appId, TimeUnit.SECONDS.toMillis(50));
        //System.in.read();
    }

    @Test
    public void test5List() throws Exception {
        bootstrap.run(cmdArgs("-status"));
        //System.in.read();
    }

    @Test
    public void test6Stop() throws Exception {
        //System.in.read();
        bootstrap.run(cmdArgs("-stop"));
    }

    private String[] cmdArgs(String... args) {
        List<String> argsList = new ArrayList<String>();
        argsList.addAll(Arrays.asList(args));
        argsList.addAll(testEnv);
        return argsList.toArray(new String[argsList.size()]);
    }
}