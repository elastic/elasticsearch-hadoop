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

package org.elasticsearch.hadoop.gradle.fixture

import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.elasticsearch.gradle.testclusters.TestDistribution
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.TaskCollection
import org.gradle.process.CommandLineArgumentProvider

/**
 * Plugin that adds the ability to stand up an Elasticsearch cluster for tests.
 * Adapted mostly from the existing cluster testing functionality from the core
 * Elasticsearch project.
 *
 * This is mostly adapted from the main Elasticsearch project, but slimmed down to
 * avoid all the extra baggage of dealing with mixed version clusters or multiple
 * nodes.
 *
 */
class ElasticsearchFixturePlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply(TestClustersPlugin)
        def version = project.hasProperty("es.version") ? project.getProperty("es.version") : project.elasticsearchVersion

        // Optionally allow user to disable the fixture
        def useFixture = Boolean.parseBoolean(project.findProperty("tests.fixture.es.enable") ?: "true")

        def integrationTestTasks = project.tasks.withType(StandaloneRestIntegTestTask)
        if (useFixture) {
            // Depends on project already containing an "integrationTest"
            // task, as well as javaHome+runtimeJavaHome configured
            createClusterFor(integrationTestTasks, project, version)
        } else {
            integrationTestTasks.all { systemProperty("test.disable.local.es", "true") }
        }
    }

    private static void createClusterFor(TaskCollection<StandaloneRestIntegTestTask> integrationTests, Project project, String version) {
        def clustersContainer = project.extensions.getByName(TestClustersPlugin.EXTENSION_NAME) as NamedDomainObjectContainer<ElasticsearchCluster>
        def integTestCluster = clustersContainer.create("integTest") { ElasticsearchCluster cluster ->
            cluster.version = version
            cluster.testDistribution = TestDistribution.DEFAULT
        }

        integrationTests.all { StandaloneRestIntegTestTask integrationTest ->
            integrationTest.useCluster(integTestCluster)
            // Add the cluster HTTP URI as a system property which isn't tracked as a task input
            integrationTest.jvmArgumentProviders.add({ ["-Dtests.rest.cluster=${integTestCluster.httpSocketURI}"] } as CommandLineArgumentProvider)
        }

        // Version settings
        def majorVersion = version.tokenize(".").get(0).toInteger()

        // Version specific configurations
        if (majorVersion <= 2) {
            integTestCluster.setting("transport.type","local")
            integTestCluster.setting("http.type","netty3")
            integTestCluster.setting("script.inline", "true")
            integTestCluster.setting("script.indexed", "true")
        } else if (majorVersion == 5) {
            integTestCluster.setting("transport.type","netty4")
            integTestCluster.setting("http.type","netty4")
            integTestCluster.setting("script.inline", "true")
            integTestCluster.setting("node.ingest", "true")
            integTestCluster.setting("script.max_compilations_rate", null)
        } else if (majorVersion == 6) {
            integTestCluster.setting("node.ingest", "true")
            integTestCluster.setting("http.host", "localhost")
            integTestCluster.systemProperty('es.http.cname_in_publish_address', 'true')
        } else if (majorVersion == 7) {
            integTestCluster.setting("node.ingest", "true")
            integTestCluster.setting("http.host", "localhost")
            integTestCluster.systemProperty('es.http.cname_in_publish_address', 'true')
        } else if (majorVersion >= 8) {
            integTestCluster.setting("node.roles", "[\"master\", \"data\", \"ingest\"]")
            integTestCluster.setting("http.host", "localhost")
            // TODO: Remove this when this is the default in 7
            integTestCluster.systemProperty('es.http.cname_in_publish_address', 'true')
            // Minimal Security
            integTestCluster.setting('xpack.security.enabled', 'true')
            integTestCluster.keystore('bootstrap.password', 'password')
            integTestCluster.user(username: 'elastic-admin', password: 'elastic-password', role: 'superuser')
        }

        // Also write a script to a file for use in tests
        File scriptsDir = new File(project.buildDir, 'scripts')
        scriptsDir.mkdirs()
        File script = null
        if (majorVersion <= 2) {
            scriptsDir.mkdirs()
            script = new File(scriptsDir, "increment.groovy").setText("ctx._source.counter+=1", 'UTF-8')
        } else if (majorVersion == 5) {
            scriptsDir.mkdirs()
            script = new File(scriptsDir, "increment.painless").setText("ctx._source.counter = ctx._source.getOrDefault('counter', 0) + 1", 'UTF-8')
        }
        if (script != null) {
            integTestCluster.extraConfigFile("script", script)
        }
    }
}
