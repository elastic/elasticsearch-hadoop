package org.elasticsearch.hadoop.gradle.fixture

import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.test.ClusterConfiguration
import org.elasticsearch.gradle.test.ClusterFormationTasks
import org.elasticsearch.gradle.test.NodeInfo
import org.elasticsearch.hadoop.gradle.util.PlaceholderTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.execution.TaskExecutionAdapter
import org.gradle.api.tasks.TaskState
import org.gradle.util.ConfigureUtil

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

    static class ElasticsearchCluster {

        Project project
        ClusterConfiguration configuration
        List<Task> tasks = []

        ElasticsearchCluster(Project project) {
            this.project = project
            this.configuration = new ClusterConfiguration(project)
        }

        void clusterConf(Closure configClosure) {
            ConfigureUtil.configure(configClosure, configuration)
        }

        void addTask(Task task) {
            tasks.add(task)
        }
    }

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

        ElasticsearchCluster cluster = project.extensions.create("esCluster", ElasticsearchCluster.class, project)
        cluster.tasks.add(integrationTest)
        ClusterConfiguration clusterConfig = cluster.configuration

        // default settings:
        clusterConfig.clusterName = "elasticsearch-fixture"
        clusterConfig.numNodes = 1
        clusterConfig.httpPort = 9500
        clusterConfig.transportPort = 9600
        clusterConfig.distribution = "default" // Full Distribution

        // Set BWC if not current ES version:
        if (version != project.elasticsearchVersion) {
            clusterConfig.bwcVersion = Version.fromString(version)
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
            clusterConfig.setting("script.max_compilations_rate", null)
        } else if (majorVersion == 6) {
            clusterConfig.setting("node.ingest", "true")
            clusterConfig.setting("http.host", "localhost")
            clusterConfig.systemProperty('es.http.cname_in_publish_address', 'true')
        } else if (majorVersion >= 7) {
            clusterConfig.setting("node.ingest", "true")
            clusterConfig.setting("http.host", "localhost")
            // TODO: Remove this when this is the default in 7
            clusterConfig.systemProperty('es.http.cname_in_publish_address', 'true')
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
            clusterConfig.extraConfigFile("script", script)
        }

        project.gradle.projectsEvaluated {
            Task clusterMain = new PlaceholderTask()
            List<NodeInfo> nodes = ClusterFormationTasks.setup(project, "esCluster", clusterMain, clusterConfig)
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
            for (Task clusterTask : cluster.tasks) {
                for (Object dependency : clusterMain.taskDeps) {
                    clusterTask.dependsOn(dependency)
                }
                for (Object finalizer : clusterMain.taskFinalizers) {
                    clusterTask.finalizedBy(finalizer)
                }
                clusterTask.doFirst {
                    project.gradle.addListener(logDumpListener)
                }
                clusterTask.doLast {
                    project.gradle.removeListener(logDumpListener)
                }
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
