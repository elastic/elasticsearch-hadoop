package org.elasticsearch.hadoop.gradle

import org.apache.tools.ant.taskdefs.condition.Os
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.DependencyResolveDetails
import org.gradle.api.artifacts.DependencySubstitutions
import org.gradle.api.artifacts.ResolutionStrategy
import org.gradle.api.artifacts.maven.MavenPom
import org.gradle.api.artifacts.maven.MavenResolver
import org.gradle.api.file.CopySpec
import org.gradle.api.java.archives.Manifest
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.MavenPlugin
import org.gradle.api.plugins.MavenPluginConvention
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.Upload
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.TestReport
import org.gradle.external.javadoc.JavadocOutputLevel
import org.gradle.external.javadoc.MinimalJavadocOptions
import org.gradle.internal.jvm.Jvm
import org.gradle.plugins.ide.eclipse.EclipsePlugin
import org.gradle.plugins.ide.idea.IdeaPlugin
import org.gradle.process.ExecResult
import org.springframework.build.gradle.propdep.PropDepsEclipsePlugin
import org.springframework.build.gradle.propdep.PropDepsIdeaPlugin
import org.springframework.build.gradle.propdep.PropDepsMavenPlugin
import org.springframework.build.gradle.propdep.PropDepsPlugin

import java.util.regex.Matcher

class BuildPlugin implements Plugin<Project>  {

    @Override
    void apply(Project project) {
        greet(project)
        configurePlugins(project)
        configureVersions(project)
        configureRuntimeSettings(project)
        configureRepositories(project)
        configureDependencies(project)
        configureBuildTasks(project)
        configureEclipse(project)
        configureMaven(project)
        configureIntegrationTestTask(project)
        configureTestReports(project)
    }

    /**
     * Say hello!
     */
    private static void greet(Project project) {
        if (!project.rootProject.hasProperty('versionsConfigured') && !project.rootProject.hasProperty('shush')) {
            println '==================================='
            println 'ES-Hadoop Build Hamster says Hello!'
            println '==================================='
        }
    }
    /**
     * Ensure that all common plugins required for the build to work are applied.
     * @param project to be configured
     */
    private static void configurePlugins(Project project) {
        // Every project will need Java in it for the time being.
        project.getPluginManager().apply(JavaPlugin.class)

        // IDE Support
        project.getPluginManager().apply(IdeaPlugin.class)
        project.getPluginManager().apply(EclipsePlugin.class)

        // Maven Support
        project.getPluginManager().apply(MavenPlugin.class)

        // Support for modeling provided/optional dependencies
        project.getPluginManager().apply(PropDepsPlugin.class)
        project.getPluginManager().apply(PropDepsIdeaPlugin.class)
        project.getPluginManager().apply(PropDepsEclipsePlugin.class)
        project.getPluginManager().apply(PropDepsMavenPlugin.class)
    }

    /**
     * Extract version information and load it into the build's extra settings
     * @param project to be configured
     */
    private static void configureVersions(Project project) {
        if (!project.rootProject.ext.has('versionsConfigured')) {
            project.rootProject.version = VersionProperties.ESHADOOP_VERSION
            println "Building version [${project.rootProject.version}]"

            project.rootProject.ext.eshadoopVersion = VersionProperties.ESHADOOP_VERSION
            project.rootProject.ext.elasticsearchVersion = VersionProperties.ELASTICSEARCH_VERSION
            project.rootProject.ext.luceneVersion = org.elasticsearch.gradle.VersionProperties.lucene
            project.rootProject.ext.versions = VersionProperties.VERSIONS
            project.rootProject.ext.versionsConfigured = true

            println "Testing against Elasticsearch [${project.rootProject.ext.elasticsearchVersion}] with Lucene [${project.rootProject.ext.luceneVersion}]"

            // Hadoop versions
            project.rootProject.ext.hadoopClient = []
            project.rootProject.ext.hadoopDistro = project.hasProperty("distro") ? project.getProperty("distro") : "hadoopStable"

            switch (project.rootProject.ext.hadoopDistro) {
                // Hadoop YARN/2.0.x
                case "hadoopYarn":
                    String version = project.hadoop2Version
                    project.rootProject.ext.hadoopVersion = version
                    project.rootProject.ext.hadoopClient = ["org.apache.hadoop:hadoop-client:$version"]
                    println "Using Apache Hadoop on YARN [$version]"
                    break

                default:
                    String version = project.hadoop22Version
                    project.rootProject.ext.hadoopVersion = version
                    project.rootProject.ext.hadoopClient = ["org.apache.hadoop:hadoop-client:$version"]
                    println "Using Apache Hadoop [$version]"
            }
        }
        project.ext.eshadoopVersion = project.rootProject.ext.eshadoopVersion
        project.ext.elasticsearchVersion = project.rootProject.ext.elasticsearchVersion
        project.ext.luceneVersion = project.rootProject.ext.luceneVersion
        project.ext.versions = project.rootProject.ext.versions
        project.ext.hadoopVersion = project.rootProject.ext.hadoopVersion
        project.ext.hadoopClient = project.rootProject.ext.hadoopClient
        project.version = project.rootProject.version
    }

    /**
     * Determine dynamic or runtime-based information and load it into the build's extra settings
     * @param project to be configured
     */
    private static void configureRuntimeSettings(Project project) {
        if (!project.rootProject.ext.has('settingsConfigured')) {
            project.rootProject.ext.java8 = JavaVersion.current().isJava8Compatible()

            String javaHome = findJavaHome()
            // Register the currently running JVM version under its version number.
            final Map<Integer, String> javaVersions = [:]
            javaVersions.put(Integer.parseInt(JavaVersion.current().getMajorVersion()), javaHome)

            project.rootProject.ext.javaHome = javaHome
            project.rootProject.ext.runtimeJavaHome = javaHome
            project.rootProject.ext.javaVersions = javaVersions

            File gitHead = gitBranch(project)
            project.rootProject.ext.gitHead = gitHead
            project.rootProject.ext.revHash = gitHash(gitHead)
            project.rootProject.ext.settingsConfigured = true

            String inFipsJvmScript = 'print(java.security.Security.getProviders()[0].name.toLowerCase().contains("fips"));'
            project.rootProject.ext.inFipsJvm = Boolean.parseBoolean(runJavascript(project, javaHome, inFipsJvmScript))
        }
        project.ext.java8 = project.rootProject.ext.java8
        project.ext.gitHead = project.rootProject.ext.gitHead
        project.ext.revHash = project.rootProject.ext.revHash
        project.ext.javaVersions = project.rootProject.ext.javaVersions
        project.ext.inFipsJvm = project.rootProject.ext.inFipsJvm
    }

    /**
     * Add all the repositories needed to pull dependencies for the build
     * @param project to be configured
     */
    private static void configureRepositories(Project project) {
        project.repositories.mavenCentral()
        project.repositories.maven { url "https://conjars.org/repo" }
        project.repositories.maven { url "https://clojars.org/repo" }
        project.repositories.maven { url 'https://repo.spring.io/plugins-release' }

        // For Elasticsearch snapshots.
        project.repositories.maven { url "https://snapshots.elastic.co/maven/" } // default
        project.repositories.maven { url "https://oss.sonatype.org/content/repositories/snapshots" } // oss-only

        // Elastic artifacts
        project.repositories.maven { url "https://artifacts.elastic.co/maven/" } // default
        project.repositories.maven { url "https://oss.sonatype.org/content/groups/public/" } // oss-only

        // For Lucene Snapshots, Use the lucene version interpreted from elasticsearch-build-tools version file.
        if (project.ext.luceneVersion.contains('-snapshot')) {
            // Extract the revision number of the snapshot via regex:
            String revision = (project.ext.luceneVersion =~ /\w+-snapshot-([a-z0-9]+)/)[0][1]
            project.repositories.maven {
                name 'lucene-snapshots'
                url "http://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/${revision}"
            }
        }
    }

    /**
     * Add all common build dependencies to the project, and provide any common resolution strategies
     * @param project to be configured
     */
    private static void configureDependencies(Project project) {
        // Create an itest source set, which will set up itest based configurations
        SourceSetContainer sourceSets = project.sourceSets as SourceSetContainer
        sourceSets.create('itest')

        // Detail all common dependencies
        project.dependencies {
            testCompile "junit:junit:${project.ext.junitVersion}"
            testCompile "org.hamcrest:hamcrest-all:${project.ext.hamcrestVersion}"

            testCompile("org.elasticsearch:elasticsearch:${project.ext.elasticsearchVersion}") {
                exclude group: "org.apache.logging.log4j", module: "log4j-api"
                exclude group: "org.elasticsearch", module: "elasticsearch-cli"
                exclude group: "org.elasticsearch", module: "elasticsearch-core"
                exclude group: "org.elasticsearch", module: "elasticsearch-secure-sm"
            }

            testRuntime "org.slf4j:slf4j-log4j12:1.7.6"
            testRuntime "org.apache.logging.log4j:log4j-api:${project.ext.log4jVersion}"
            testRuntime "org.apache.logging.log4j:log4j-core:${project.ext.log4jVersion}"
            testRuntime "org.apache.logging.log4j:log4j-1.2-api:${project.ext.log4jVersion}"
            testRuntime "net.java.dev.jna:jna:4.2.2"
            testCompile "org.codehaus.groovy:groovy:${project.ext.groovyVersion}:indy"
            testRuntime "org.locationtech.spatial4j:spatial4j:0.6"
            testRuntime "com.vividsolutions:jts:1.13"

            // TODO: Remove when we merge ITests to test dirs
            itestCompile("org.apache.hadoop:hadoop-minikdc:${project.ext.minikdcVersion}") {
                // For some reason, the dependencies that are pulled in with MiniKDC have multiple resource files
                // that cause issues when they are loaded. We exclude the ldap schema data jar to get around this.
                exclude group: "org.apache.directory.api", module: "api-ldap-schema-data"
            }
            itestCompile project.sourceSets.main.output
            itestCompile project.configurations.testCompile
            itestCompile project.configurations.provided
            itestCompile project.sourceSets.test.output
            itestRuntime project.configurations.testRuntime
        }

        // Deal with the messy conflicts out there
        project.configurations.all { Configuration conf ->
            conf.resolutionStrategy { ResolutionStrategy resolve ->
                // Locking on to joda 2.8
                resolve.force('joda-time:joda-time:2.8')

                // Ensure our jackson version is respected in the transient deps
                resolve.force("org.codehaus.jackson:jackson-mapper-asl:${project.ext.jacksonVersion}")
                resolve.force("org.codehaus.jackson:jackson-core-asl:${project.ext.jacksonVersion}")

                // force the use of commons-http from Hadoop
                resolve.force('commons-httpclient:commons-httpclient:3.0.1')

                // used when using Elastic non-shaded version
                resolve.force("commons-cli:commons-cli:1.2")

                resolve.eachDependency { DependencyResolveDetails details ->
                    // There are tons of slf4j-* variants. Search for all of them, and lock them down.
                    if (details.requested.name.contains("slf4j-")) {
                        details.useVersion "1.7.6"
                    }
                    // Be careful with log4j version settings as they can be easily missed.
                    if (details.requested.name.contains("org.apache.logging.log4j") && details.requested.name.contains("log4j-")) {
                        details.useVersion project.ext.log4jVersion
                    }
                    // Convert any references to the servlet-api into the jetty servlet artifact.
                    if (details.requested.name.equals("servlet-api")) {
                        details.useTarget group: "org.eclipse.jetty.orbit", name: "javax.servlet", version: "3.0.0.v201112011016"
                    }

                }
            }
        }

        // Do substitutions for ES fixture downloads
        project.configurations.all { Configuration configuration ->
            configuration.resolutionStrategy.dependencySubstitution { DependencySubstitutions subs ->
                subs.substitute(subs.module("downloads.zip:elasticsearch:${project.ext.elasticsearchVersion}"))
                        .with(subs.module("org.elasticsearch.distribution.zip:elasticsearch:${project.ext.elasticsearchVersion}"))
            }
        }
    }

    /**
     * Configure any common properties/tasks of the Java/IDE/etc build plugins
     * @param project to be configured
     */
    private static void configureBuildTasks(Project project) {
        // Target Java 1.8 compilation
        JavaCompile compileJava = project.tasks.getByName('compileJava') as JavaCompile
        compileJava.setSourceCompatibility('1.8')
        compileJava.setTargetCompatibility('1.8')
        compileJava.getOptions().setCompilerArgs(['-Xlint:unchecked', '-Xlint:options'])

        // Target Java 1.8 for tests
        JavaCompile compileTestJava = project.tasks.getByName('compileTestJava') as JavaCompile
        compileTestJava.setSourceCompatibility('1.8')
        compileTestJava.setTargetCompatibility('1.8')

        // Enable HTML test reports
        Test testTask = project.tasks.getByName('test') as Test
        testTask.getReports().getByName('html').setEnabled(true)

        // Configure project jar task with manifest and include license and notice data.
        Jar jar = project.tasks.getByName('jar') as Jar

        Manifest manifest = jar.getManifest()
        manifest.attributes["Created-By"] = "${System.getProperty("java.version")} (${System.getProperty("java.specification.vendor")})"
        manifest.attributes['Implementation-Title'] = project.name
        manifest.attributes['Implementation-Version'] = project.version
        manifest.attributes['Implementation-URL'] = "https://github.com/elastic/elasticsearch-hadoop"
        manifest.attributes['Implementation-Vendor'] = "Elastic"
        manifest.attributes['Implementation-Vendor-Id'] = "org.elasticsearch.hadoop"
        manifest.attributes['Repository-Revision'] = project.ext.revHash
        String build = System.env['ESHDP.BUILD']
        if (build != null) {
            manifest.attributes['Build'] = build
        }

        jar.from("${project.rootDir}/docs/src/info") { CopySpec spec ->
            spec.include("license.txt")
            spec.include("notice.txt")
            spec.into("META-INF")
            spec.expand(copyright: new Date().format('yyyy'), version: project.version)
        }

        // Jar up the sources of the project
        Jar sourcesJar = project.tasks.create('sourcesJar', Jar)
        sourcesJar.dependsOn(project.tasks.classes)
        sourcesJar.classifier = 'sources'
        sourcesJar.from(project.sourceSets.main.allSource)

        // Configure javadoc
        Javadoc javadoc = project.tasks.getByName('javadoc') as Javadoc
        javadoc.title = "${project.rootProject.description} ${project.version} API"
        javadoc.excludes = [
                "org/elasticsearch/hadoop/mr/compat/**",
                "org/elasticsearch/hadoop/rest/**",
                "org/elasticsearch/hadoop/serialization/**",
                "org/elasticsearch/hadoop/util/**",
                "org/apache/hadoop/hive/**"
        ]

        MinimalJavadocOptions javadocOptions = javadoc.getOptions()
        javadocOptions.docFilesSubDirs = true
        javadocOptions.outputLevel = JavadocOutputLevel.QUIET
        javadocOptions.breakIterator = true
        javadocOptions.author = false
        javadocOptions.header = project.name
        javadocOptions.showFromProtected()
        if (project.ext.java8) {
            javadocOptions.addStringOption('Xdoclint:none', '-quiet')
        }
        javadocOptions.groups = [
                'Elasticsearch Map/Reduce' : ['org.elasticsearch.hadoop.mr*'],
                'Elasticsearch Hive' : ['org.elasticsearch.hadoop.hive*'],
                'Elasticsearch Pig' : ['org.elasticsearch.hadoop.pig*'],
                'Elasticsearch Spark' : ['org.elasticsearch.spark*'],
                'Elasticsearch Storm' : ['org.elasticsearch.storm*'],
        ]
        javadocOptions.links = [ // External doc links
                "https://docs.oracle.com/javase/6/docs/api/",
                "https://commons.apache.org/proper/commons-logging/apidocs/",
                "https://hadoop.apache.org/docs/stable2/api/",
                "https://pig.apache.org/docs/r0.15.0/api/",
                "https://hive.apache.org/javadocs/r1.2.2/api/",
                "https://spark.apache.org/docs/latest/api/java/",
                "https://storm.apache.org/releases/current/javadocs/"
        ]

        // Package up the javadocs into their own jar
        Jar javadocJar = project.tasks.create('javadocJar', Jar)
        javadocJar.classifier = 'javadoc'
        javadocJar.from(project.tasks.javadoc)

        // Task for creating ALL of a project's jars - Like assemble, but this includes the sourcesJar and javadocJar.
        Task pack = project.tasks.create('pack')
        pack.dependsOn(project.tasks.jar)
        pack.dependsOn(javadocJar)
        pack.dependsOn(sourcesJar)
        pack.outputs.files(project.tasks.jar.archivePath, javadocJar.archivePath, sourcesJar.archivePath)

        // The distribution task is like assemble, but packages up a lot of extra jars and performs extra tasks that
        // are mostly used for snapshots and releases.
        Task distribution = project.tasks.create('distribution')
        distribution.dependsOn(pack)
        // Co-locate all build artifacts into distributions subdir for easier build automation
        distribution.doLast {
            project.copy { CopySpec spec ->
                spec.from(jar.archivePath)
                spec.from(javadocJar.archivePath)
                spec.from(sourcesJar.archivePath)
                spec.into("${project.buildDir}/distributions")
            }
        }
    }

    private static void configureEclipse(Project project) {
        // TODO: Is this still required on modern Eclipse versions?
        // adding the M/R project creates duplicates in the Eclipse CP so here we filter them out
        // the lib entries with sources seem to be placed first so they 'win' over those w/o sources
        project.eclipse {
            classpath.file {
                whenMerged { cp ->
                    entries.unique { a, b ->
                        return a.path.compareTo(b.path)
                    }
                    entries.removeAll { it.path.endsWith('.pom') }
                }
            }
            jdt {
                javaRuntimeName = "JavaSE-1.8"
                sourceCompatibility = 1.8
                targetCompatibility = 1.8
            }
        }
    }

    private static void configureMaven(Project project) {
        Task writePom = project.getTasks().create('writePom')
        writePom.doLast {
            MavenPluginConvention convention = project.getConvention().getPlugins().get('maven') as MavenPluginConvention
            MavenPom pom = customizePom(convention.pom(), project)
            pom.writeTo("${project.buildDir}/distributions/${project.archivesBaseName}-${project.version}.pom")
        }

        // Write the pom when building a distribution.
        Task distribution = project.getTasks().getByName('distribution')
        distribution.dependsOn(writePom)

        // Get the task that installs to local maven repo. Instruct the installation resolver to use our custom pom.
        Upload mavenInstallTask = project.getTasks().getByName('install') as Upload
        MavenResolver installResolver = mavenInstallTask.repositories.mavenInstaller as MavenResolver
        installResolver.setPom(customizePom(installResolver.getPom(), project))
    }

    /**
     * Given a maven pom, customize it for our project's using the information provided by the given project.
     * @param pom
     * @param gradleProject
     * @return
     */
    private static MavenPom customizePom(MavenPom pom, Project gradleProject) {
        // Maven does most of the lifting to translate a Project into a MavenPom
        // Run this closure after that initial boilerplate configuration is done
        pom.whenConfigured { MavenPom generatedPom ->

            // eliminate test-scoped dependencies (no need in maven central poms)
            generatedPom.dependencies.removeAll { dep ->
                dep.scope == 'test' || dep.artifactId == 'elasticsearch-hadoop-mr'
            }

            // Mark the optional dependencies to actually be optional
            generatedPom.dependencies.findAll { it.scope == 'optional' }.each {
                it.optional = "true"
            }

            // By default propdeps models optional dependencies as compile/optional
            // for es-hadoop optional is best if these are modeled as provided/optional
            generatedPom.dependencies.findAll { it.optional == "true" }.each {
                it.scope = "provided"
            }

            // Storm hosts their jars outside of maven central.
            boolean storm = generatedPom.dependencies.any { it.groupId == 'org.apache.storm' }

            if (storm)
                generatedPom.project {
                    repositories {
                        repository {
                            id = 'clojars.org'
                            url = 'https://clojars.org/repo'
                        }
                    }
                }

            // add all items necessary for publication
            generatedPom.project {
                name = gradleProject.description
                description = gradleProject.description
                url = 'http://github.com/elastic/elasticsearch-hadoop'
                organization {
                    name = 'Elastic'
                    url = 'https://www.elastic.co/'
                }
                licenses {
                    license {
                        name = 'The Apache Software License, Version 2.0'
                        url = 'https://www.apache.org/licenses/LICENSE-2.0.txt'
                        distribution = 'repo'
                    }
                }
                scm {
                    url = 'https://github.com/elastic/elasticsearch-hadoop'
                    connection = 'scm:git:git://github.com/elastic/elasticsearch-hadoop'
                    developerConnection = 'scm:git:git://github.com/elastic/elasticsearch-hadoop'
                }
                developers {
                    developer {
                        id = 'jbaiera'
                        name = 'James Baiera'
                        email = 'james.baiera@elastic.co'
                    }
                    developer {
                        id = 'costin'
                        name = 'Costin Leau'
                        email = 'costin@elastic.co'
                    }
                }
            }

            groupId = "org.elasticsearch"
            artifactId = gradleProject.archivesBaseName
        }
        return pom
    }

    /**
     * Create a task specifically for integration tests, add the integration test code to the testing uber-jar,
     * and configure a local Elasticsearch node for use as a test fixture.
     * @param project to be configured
     */
    private static void configureIntegrationTestTask(Project project) {
        Jar hadoopTestingJar = project.rootProject.tasks.findByName('hadoopTestingJar') as Jar
        if (hadoopTestingJar == null) {
            // jar used for testing Hadoop remotely (es-hadoop + tests)
            hadoopTestingJar = project.rootProject.tasks.create('hadoopTestingJar', Jar)
            hadoopTestingJar.dependsOn(project.rootProject.tasks.getByName('jar'))
            hadoopTestingJar.classifier = 'testing'
            project.logger.info("Created Remote Testing Jar")
        }

        // Add this project's classes to the testing uber-jar
        hadoopTestingJar.from(project.sourceSets.test.output)
        hadoopTestingJar.from(project.sourceSets.main.output)
        hadoopTestingJar.from(project.sourceSets.itest.output)

        Test integrationTest = project.tasks.create('integrationTest', Test.class)
        integrationTest.dependsOn(hadoopTestingJar)

        integrationTest.testClassesDirs = project.sourceSets.itest.output.classesDirs
        integrationTest.classpath = project.sourceSets.itest.runtimeClasspath
        integrationTest.excludes = ["**/Abstract*.class"]

        integrationTest.ignoreFailures = false

        integrationTest.minHeapSize = "256m"
        integrationTest.maxHeapSize = "2g"
        if (!project.ext.java8) {
            integrationTest.jvmArgs '-XX:MaxPermSize=496m'
        }

        integrationTest.testLogging {
            displayGranularity 0
            events "started", "failed" //, "standardOut", "standardError"
            exceptionFormat "full"
            showCauses true
            showExceptions true
            showStackTraces true
            stackTraceFilters "groovy"
            minGranularity 2
            maxGranularity 2
        }

        integrationTest.reports.html.enabled = false

        // Only add cluster settings if it's not the root project
        if (project != project.rootProject) {
            project.logger.info "Configuring ${project.name} integrationTest task to use ES Fixture"
            // Create the cluster fixture around the integration test.
            // There's probably a more elegant way to do this in Gradle
            project.plugins.apply("es.hadoop.cluster")
        }
    }

    /**
     * Configure the root testReport task with the test tasks in this project to report on, creating the report task
     * on root if it is not created yet.
     * @param project to configure
     */
    private static void configureTestReports(Project project) {
        TestReport testReport = project.rootProject.getTasks().findByName('testReport') as TestReport
        if (testReport == null) {
            // Create the task on root if it is not created yet.
            testReport = project.rootProject.getTasks().create('testReport', TestReport.class)
            testReport.setDestinationDir(project.rootProject.file("${project.rootProject.getBuildDir()}/reports/allTests"))
        }
        testReport.reportOn(project.getTasks().getByName('test'))
        testReport.reportOn(project.getTasks().getByName('integrationTest'))
    }

    /**
     * @param project that belongs to a git repo
     * @return the file containing the hash for the current branch
     */
    private static File gitBranch(Project project) {
        // parse the git files to find out the revision
        File gitHead =  project.file("${project.rootDir}/.git/HEAD")
        if (gitHead != null && !gitHead.exists()) {
            // Try as a sub module
            File subModuleGit = project.file("${project.rootDir}/.git")
            if (subModuleGit != null && subModuleGit.exists()) {
                String content = subModuleGit.text.trim()
                if (content.startsWith("gitdir:")) {
                    gitHead = project.file("${project.rootDir}/" + content.replace('gitdir: ','') + "/HEAD")
                }
            }
        }

        if (gitHead != null && gitHead.exists()) {
            String content = gitHead.text.trim()
            if (content.startsWith("ref:")) {
                return project.file("${project.rootDir}/.git/" + content.replace('ref: ',''))
            }
            return gitHead
        }
        return null
    }

    /**
     * @param gitHead file containing the the currently checked out ref
     * @return the current commit version hash
     */
    private static String gitHash(File gitHead) {
        String rev = "unknown"

        if (gitHead.exists()) {
            rev = gitHead.text.trim()
        }
        return rev
    }

    /**
     * @return the location of the JDK for the currently executing JVM
     */
    private static String findJavaHome() {
        String javaHome = System.getenv('JAVA_HOME')
        if (javaHome == null) {
            if (System.getProperty("idea.active") != null || System.getProperty("eclipse.launcher") != null) {
                // intellij doesn't set JAVA_HOME, so we use the jdk gradle was run with
                javaHome = Jvm.current().javaHome
            } else {
                throw new GradleException('JAVA_HOME must be set to build Elasticsearch')
            }
        }
        return javaHome
    }

    /** Runs the given javascript using jjs from the jdk, and returns the output */
    private static String runJavascript(Project project, String javaHome, String script) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream()
        ByteArrayOutputStream stderr = new ByteArrayOutputStream()
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // gradle/groovy does not properly escape the double quote for windows
            script = script.replace('"', '\\"')
        }
        File jrunscriptPath = new File(javaHome, 'bin/jrunscript')
        ExecResult result = project.exec {
            executable = jrunscriptPath
            args '-e', script
            standardOutput = stdout
            errorOutput = stderr
            ignoreExitValue = true
        }
        if (result.exitValue != 0) {
            project.logger.error("STDOUT:")
            stdout.toString('UTF-8').eachLine { line -> project.logger.error(line) }
            project.logger.error("STDERR:")
            stderr.toString('UTF-8').eachLine { line -> project.logger.error(line) }
            result.rethrowFailure()
        }
        return stdout.toString('UTF-8').trim()
    }
}
