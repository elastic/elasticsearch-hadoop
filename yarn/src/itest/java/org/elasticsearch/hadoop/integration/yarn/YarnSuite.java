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

import java.io.File;
import java.io.FilenameFilter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.elasticsearch.hadoop.yarn.client.ClientRpc;
import org.elasticsearch.hadoop.yarn.util.Assert;
import org.elasticsearch.hadoop.yarn.util.PropertiesUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.rules.ChainedExternalResource;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ YarnTest.class })
@Ignore("Classpath madness")
public class YarnSuite {

    public static ClientRpc YC;
    public static Properties TEST_PROPS = PropertiesUtils.load(YarnSuite.class, "/yarn-test.properties");
    public static YarnConfiguration CFG = new YarnConfiguration();

    static {
        CFG.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, -1);
    }

    public static YarnTestCluster CLUSTER = new YarnTestCluster(CFG);
    public static DistributedFileSystem FS;
    public static File CLIENT_JAR;

    public static ExternalResource YARN_CLIENT = new ExternalResource() {

        @Override
        protected void before() throws Throwable {
            YC = new ClientRpc(CFG);
            YC.start();
        }

        @Override
        protected void after() {
            YC.close();
        }
    };

    public static ExternalResource PROVISION_JARS = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            // initialize FS (now that the cluster has started)
            FS = CLUSTER.fs();

            File libs = new File("build/libs");
            File[] clientLibs = libs.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.contains("-yarn");
                }
            });

            Assert.isTrue(clientLibs.length == 1, "Check there is exactly one (client) jar " + Arrays.toString(clientLibs));
            CLIENT_JAR = clientLibs[0];

            //TEST_PROPS.setProperty("hdfs.esyarn.jar", CLIENT_JAR.getAbsolutePath());

            StringWriter sw = new StringWriter();
            Configuration.dumpConfiguration(CFG, sw);
            System.out.println("Configuration before starting the test " + sw);
        }
    };

    @ClassRule
    public static ExternalResource RES = new ChainedExternalResource(CLUSTER, PROVISION_JARS, YARN_CLIENT);

}