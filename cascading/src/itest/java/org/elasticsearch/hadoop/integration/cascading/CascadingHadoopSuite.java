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
package org.elasticsearch.hadoop.integration.cascading;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.HdfsUtils;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.LocalEs;
import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import cascading.util.Update;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractCascadingHadoopSaveTest.class, AbstractCascadingHadoopSearchTest.class })
//@Suite.SuiteClasses({ CascadingHadoopSearchTest.class })
public class CascadingHadoopSuite {

    @ClassRule
    public static ExternalResource resource = new LocalEs();

    public static JobConf configuration;

    @BeforeClass
    public static void setup() throws Exception {
        configuration = HdpBootstrap.hadoopConfig();
        System.setProperty(Update.UPDATE_CHECK_SKIP, "true");

        //configuration.setNumReduceTasks(0);
        if (!HadoopCfgUtils.isLocal(configuration)) {
            configuration = CascadingProvisioner.cascading(configuration);
        }
        HdfsUtils.copyFromLocal(TestUtils.sampleArtistsDat());
        HdfsUtils.copyFromLocal(TestUtils.sampleQueryDsl());
        HdfsUtils.copyFromLocal(TestUtils.sampleQueryUri());

        QueryTestParams.provisionQueries(configuration);
    }

    private static class CascadingProvisioner extends Provisioner {
        public static final String[] CASCADING_JARS;

        static {
            // initialize cascading jars
            // read them from the classpath

            List<String> jars = new ArrayList<String>();
            // cascading-core
            jars.add(findContainingJar("cascading/cascade/Cascade.class"));
            // cascading-hadoop
            jars.add(findContainingJar("cascading/flow/hadoop/HadoopFlow.class"));
            // jgrapht
            jars.add(findContainingJar("org/jgrapht/Graph.class"));
            jars.add(findContainingJar("riffle/process/Process.class"));
            // riffle
            jars.add(findContainingJar("org/codehaus/janino/Java.class"));
            // janino commons-compiler
            jars.add(findContainingJar("org/codehaus/commons/compiler/CompileException.class"));

            CASCADING_JARS = jars.toArray(new String[jars.size()]);
        }

        public static JobConf cascading(JobConf conf) {
            addLibs(conf, CASCADING_JARS);
            return conf;
        }

        private static String findContainingJar(String binaryName) {
            ClassLoader loader = Provisioner.class.getClassLoader();

            try {
                for (Enumeration<URL> urls = loader.getResources(binaryName); urls.hasMoreElements();) {
                    URL url = urls.nextElement();
                    // remove jar:
                    if ("jar".equals(url.getProtocol())) {
                        return url.getPath().replaceAll("!.*$", "");
                    }
                }
            } catch (IOException ex) {
                throw new EsHadoopIllegalArgumentException("Cannot find jar for class " + binaryName, ex);
            }
            throw new EsHadoopIllegalArgumentException("Cannot find class " + binaryName);
        }
    }
}
