package org.elasticsearch.hadoop.gradle.fixture

import org.elasticsearch.gradle.test.ClusterConfiguration
import org.elasticsearch.gradle.test.ClusterFormationTasks
import org.elasticsearch.gradle.test.NodeInfo
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.execution.TaskExecutionAdapter
import org.gradle.api.tasks.TaskState

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.stream.Stream

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

        def version = project.hasProperty("es.version") ? project.getProperty("es.version") : project.elasticsearchVersion

        // Optionally allow user to disable the fixture
        def hasLocalRepo = project.hasProperty("localRepo")
        def useFixture = !hasLocalRepo && Boolean.parseBoolean(project.hasProperty("tests.fixture.es.enable") ? project.getProperty("tests.fixture.es.enable") : "true")

        if (useFixture) {
            // Depends on project already containing an "integrationTest"
            // task, as well as javaHome+runtimeJavaHome configured
            createClusterFor(project.tasks.getByName("integrationTest"), project, version)
        } else {
            project.tasks.getByName("integrationTest") {
                systemProperty "test.disable.local.es", "true"
            }
        }
    }

    private static def createClusterFor(Task integrationTest, Project project, String version) {
        // Version settings
        def majorVersion = version.tokenize(".").get(0).toInteger()

        // Init task can be used to prepare cluster
        Task clusterInit = project.tasks.create(name: "esCluster#init", dependsOn: project.testClasses)
        integrationTest.dependsOn(clusterInit)

        ClusterConfiguration clusterConfig = project.extensions.create("esCluster", ClusterConfiguration.class, project)

        // default settings:
        clusterConfig.clusterName = "elasticsearch-fixture"
        clusterConfig.numNodes = 1
        clusterConfig.httpPort = 9500
        clusterConfig.transportPort = 9600
        clusterConfig.distribution = "zip" // Full Distribution

        // Set BWC if not current ES version:
        if (version != project.elasticsearchVersion) {
            clusterConfig.bwcVersion = version
            clusterConfig.numBwcNodes = 1
        }

        // Version specific configurations
        if (majorVersion <= 2) {
            clusterConfig.setting("transport.type","local")
            clusterConfig.setting("http.type","netty3")
            clusterConfig.setting("script.inline", "true")
            clusterConfig.setting("script.indexed", "true")
        } else if (majorVersion == 5) {
            clusterConfig.setting("transport.type","netty4")
            clusterConfig.setting("http.type","netty4")
            clusterConfig.setting("script.inline", "true")
            clusterConfig.setting("node.ingest", "true")
        } else if (majorVersion >= 6) {
            clusterConfig.setting("transport.type","netty4")
            clusterConfig.setting("http.type","netty4")
            clusterConfig.setting("node.ingest", "true")
        }

        // Also write a script to a file for use in tests
        File scriptsDir = new File(project.buildDir, 'scripts')
        scriptsDir.mkdirs()
        File script
        if (majorVersion <= 2) {
            scriptsDir.mkdirs()
            script = new File(scriptsDir, "increment.groovy").setText("ctx._source.counter+=1", 'UTF-8')
        } else if (majorVersion == 5) {
            scriptsDir.mkdirs()
            script = new File(scriptsDir, "increment.painless").setText("ctx._source.counter = ctx._source.getOrDefault('counter', 0) + 1", 'UTF-8')
        }
        clusterConfig.extraConfigFile("script", script)

        project.gradle.projectsEvaluated {
            List<NodeInfo> nodes = ClusterFormationTasks.setup(project, "esCluster", integrationTest, clusterConfig)
            project.tasks.getByPath("esCluster#wait").doLast {
                integrationTest.systemProperty('tests.rest.cluster', "${nodes.collect{it.httpUri()}.join(",")}")
            }

            // dump errors and warnings from cluster log on failure
            TaskExecutionAdapter logDumpListener = new TaskExecutionAdapter() {
                @Override
                void afterExecute(Task task, TaskState state) {
                    if (state.failure != null) {
                        for (NodeInfo nodeInfo : nodes) {
                            printLogExcerpt(nodeInfo)
                        }
                    }
                }
            }
            integrationTest.doFirst {
                project.gradle.addListener(logDumpListener)
            }
            integrationTest.doLast {
                project.gradle.removeListener(logDumpListener)
            }
        }
    }

    /** Print out an excerpt of the log from the given node. */
    protected static void printLogExcerpt(NodeInfo nodeInfo) {
        File logFile = new File(nodeInfo.homeDir, "logs/${nodeInfo.clusterName}.log")
        println("\nCluster ${nodeInfo.clusterName} - node ${nodeInfo.nodeNum} log excerpt:")
        println("(full log at ${logFile})")
        println('-----------------------------------------')
        Stream<String> stream = Files.lines(logFile.toPath(), StandardCharsets.UTF_8)
        try {
            boolean inStartup = true
            boolean inExcerpt = false
            int linesSkipped = 0
            for (String line : stream) {
                if (line.startsWith("[")) {
                    inExcerpt = false // clear with the next log message
                }
                if (line =~ /(\[WARN *\])|(\[ERROR *\])/) {
                    inExcerpt = true // show warnings and errors
                }
                if (inStartup || inExcerpt) {
                    if (linesSkipped != 0) {
                        println("... SKIPPED ${linesSkipped} LINES ...")
                    }
                    println(line)
                    linesSkipped = 0
                } else {
                    ++linesSkipped
                }
                if (line =~ /recovered \[\d+\] indices into cluster_state/) {
                    inStartup = false
                }
            }
        } finally {
            stream.close()
        }
        println('=========================================')

    }
}
