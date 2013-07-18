/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.integration.hive;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.hadoop.integration.HdfsUtils;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.integration.LocalES;
import org.elasticsearch.hadoop.integration.Provisioner;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ChainedExternalResource;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ HiveSaveTest.class, HiveSearchTest.class })
//@Suite.SuiteClasses({ HiveSearchTest.class })
public class HiveSuite {

    static HiveInstance server;
    static boolean isLocal = true;

    static String cleanDdl = "DROP DATABASE IF EXISTS test CASCADE";
    static String createDB = "CREATE DATABASE test";
    static String useDB = "USE test";

    static String originalResource;
    static String hdfsResource;

    static {
        try {
            originalResource = HiveSuite.class.getClassLoader().getResource("hive-compound.dat").toURI().toString();
            hdfsResource = originalResource;
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static ExternalResource hive = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            Properties props = new TestSettings().getProperties();
            String hive = props.getProperty("hive", "local");

            isLocal = "local".equals(hive);
            server = (isLocal ? new HiveEmbeddedServer(props) : new HiveJdbc(hive));
            server.start();

            server.execute(cleanDdl);
            server.execute(createDB);
            server.execute(useDB);
        }

        @Override
        protected void after() {
            try {
                server.execute(cleanDdl);
                server.stop();
            } catch (Exception ex) {
            }
        }
    };

    @ClassRule
    public static ExternalResource resource = new ChainedExternalResource(new LocalES(), hive);

    @BeforeClass
    public static void setup() {
        if (!isLocal) {
            HdfsUtils.copyFromLocal(Provisioner.ESHADOOP_TESTING_JAR, Provisioner.HDFS_ES_HDP_LIB);
            // copy jar to DistributedCache
            try {
                DistributedCache.addArchiveToClassPath(new Path(Provisioner.HDFS_ES_HDP_LIB), HdpBootstrap.hadoopConfig());
            } catch (IOException ex) {
                throw new RuntimeException("Cannot provision Hive", ex);
            }
            hdfsResource = "/eshdp/hive/hive-compund.dat";
            HdfsUtils.copyFromLocal(originalResource, hdfsResource);
        }
    }
}