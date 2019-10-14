package org.elasticsearch.hadoop.gradle.fixture

import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.gradle.testclusters.RestTestRunnerTask
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.elasticsearch.gradle.testclusters.TestDistribution
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.Plugin
import org.gradle.api.Project
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
        def hasLocalRepo = project.hasProperty("localRepo")
        def useFixture = !hasLocalRepo && Boolean.parseBoolean(project.findProperty("tests.fixture.es.enable") ?: "true")

        def integrationTestTask = project.tasks.getByName("integrationTest") as RestTestRunnerTask
        if (useFixture) {
            // Depends on project already containing an "integrationTest"
            // task, as well as javaHome+runtimeJavaHome configured
            createClusterFor(integrationTestTask, project, version)
        } else {
            integrationTestTask.systemProperty("test.disable.local.es", "true")
        }
    }

    private static void createClusterFor(RestTestRunnerTask integrationTest, Project project, String version) {
        def clustersContainer = project.extensions.getByName(TestClustersPlugin.EXTENSION_NAME) as NamedDomainObjectContainer<ElasticsearchCluster>
        def integTestCluster = clustersContainer.create("integTest") { ElasticsearchCluster cluster ->
            cluster.version = version
            cluster.testDistribution = TestDistribution.DEFAULT
        }

        integrationTest.useCluster(integTestCluster)
        // Add the cluster HTTP URI as a system property which isn't tracked as a task input
        integrationTest.jvmArgumentProviders.add({ ["-Dtests.rest.cluster=${integTestCluster.httpSocketURI}"] } as CommandLineArgumentProvider)

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
        } else if (majorVersion >= 7) {
            integTestCluster.setting("node.ingest", "true")
            integTestCluster.setting("http.host", "localhost")
            // TODO: Remove this when this is the default in 7
            integTestCluster.systemProperty('es.http.cname_in_publish_address', 'true')
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
