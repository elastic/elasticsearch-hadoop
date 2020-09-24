package org.elasticsearch.hadoop.gradle

import org.elasticsearch.gradle.DependenciesInfoPlugin
import org.elasticsearch.gradle.info.BuildParams
import org.elasticsearch.gradle.precommit.DependencyLicensesTask
import org.elasticsearch.gradle.precommit.LicenseHeadersTask
import org.elasticsearch.gradle.precommit.UpdateShasTask
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.DependencyResolveDetails
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.ResolutionStrategy
import org.gradle.api.artifacts.maven.MavenPom
import org.gradle.api.artifacts.maven.MavenResolver
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.file.CopySpec
import org.gradle.api.file.FileCollection
import org.gradle.api.java.archives.Manifest
import org.gradle.api.plugins.JavaLibraryPlugin
import org.gradle.api.plugins.MavenPlugin
import org.gradle.api.plugins.MavenPluginConvention
import org.gradle.api.plugins.scala.ScalaPlugin
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.Upload
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.external.javadoc.JavadocOutputLevel
import org.gradle.external.javadoc.MinimalJavadocOptions
import org.gradle.plugins.ide.eclipse.EclipsePlugin
import org.gradle.plugins.ide.idea.IdeaPlugin

class BuildPlugin implements Plugin<Project>  {

    @Override
    void apply(Project project) {
        configurePlugins(project)
        configureConfigurations(project)
        configureDependencies(project)
        configureBuildTasks(project)
        configureEclipse(project)
        configureMaven(project)
        configureIntegrationTestTask(project)
        configurePrecommit(project)
        configureDependenciesInfo(project)
    }

    /**
     * Ensure that all common plugins required for the build to work are applied.
     * @param project to be configured
     */
    private static void configurePlugins(Project project) {
        // Configure global project settings
        project.getPluginManager().apply(BaseBuildPlugin.class)

        // BuildPlugin will continue to assume Java projects for the time being.
        project.getPluginManager().apply(JavaLibraryPlugin.class)

        // IDE Support
        project.getPluginManager().apply(IdeaPlugin.class)
        project.getPluginManager().apply(EclipsePlugin.class)

        // Maven Support
        project.getPluginManager().apply(MavenPlugin.class)
    }

    /** Return the configuration name used for finding transitive deps of the given dependency. */
    private static String transitiveDepConfigName(String groupId, String artifactId, String version) {
        return "_transitive_${groupId}_${artifactId}_${version}"
    }

    private static void configureConfigurations(Project project) {
        if (project != project.rootProject) {
            // Set up avenues for sharing source files between projects in order to create embedded Javadocs
            // Import source configuration
            Configuration sources = project.configurations.create("additionalSources")
            sources.canBeConsumed = false
            sources.canBeResolved = true
            sources.attributes {
                // Changing USAGE is required when working with Scala projects, otherwise the source dirs get pulled
                // into incremental compilation analysis.
                attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage, 'java-source'))
                attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, project.objects.named(LibraryElements, 'sources'))
            }

            // Export source configuration
            Configuration sourceElements = project.configurations.create("sourceElements")
            sourceElements.canBeConsumed = true
            sourceElements.canBeResolved = false
            sourceElements.extendsFrom(sources)
            sourceElements.attributes {
                // Changing USAGE is required when working with Scala projects, otherwise the source dirs get pulled
                // into incremental compilation analysis.
                attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage, 'java-source'))
                attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, project.objects.named(LibraryElements, 'sources'))
            }

            // Import javadoc sources
            Configuration javadocSources = project.configurations.create("javadocSources")
            javadocSources.canBeConsumed = false
            javadocSources.canBeResolved = true
            javadocSources.attributes {
                // Changing USAGE is required when working with Scala projects, otherwise the source dirs get pulled
                // into incremental compilation analysis.
                attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage, 'javadoc-source'))
                attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, project.objects.named(LibraryElements, 'sources'))
            }

            // Export source configuration
            Configuration javadocElements = project.configurations.create("javadocElements")
            javadocElements.canBeConsumed = true
            javadocElements.canBeResolved = false
            javadocElements.extendsFrom(sources)
            javadocElements.attributes {
                // Changing USAGE is required when working with Scala projects, otherwise the source dirs get pulled
                // into incremental compilation analysis.
                attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage, 'javadoc-source'))
                attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, project.objects.named(LibraryElements, 'sources'))
            }

            // Export configuration for archives that should be in the distribution
            Configuration distElements = project.configurations.create('distElements')
            distElements.canBeConsumed = true
            distElements.canBeResolved = false
            distElements.attributes {
                attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage, 'packaging'))
            }
        }

        if (project.path.startsWith(":qa")) {
            return
        }

        // force all dependencies added directly to compile/testCompile to be non-transitive, except for Elasticsearch projects
        Closure disableTransitiveDeps = { Dependency dep ->
            if (dep instanceof ModuleDependency && !(dep instanceof ProjectDependency) && dep.group.startsWith('org.elasticsearch') == false) {
                dep.transitive = false

                // also create a configuration just for this dependency version, so that later
                // we can determine which transitive dependencies it has
                String depConfig = transitiveDepConfigName(dep.group, dep.name, dep.version)
                if (project.configurations.findByName(depConfig) == null) {
                    project.configurations.create(depConfig)
                    project.dependencies.add(depConfig, "${dep.group}:${dep.name}:${dep.version}")
                }
            }
        }

        project.configurations.api.dependencies.all(disableTransitiveDeps)
        project.configurations.implementation.dependencies.all(disableTransitiveDeps)
        project.configurations.compileOnly.dependencies.all(disableTransitiveDeps)
        project.configurations.runtimeOnly.dependencies.all(disableTransitiveDeps)
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
            testImplementation("junit:junit:${project.ext.junitVersion}")
            testImplementation("org.hamcrest:hamcrest-all:${project.ext.hamcrestVersion}")

            testImplementation("joda-time:joda-time:2.8")

            testImplementation("org.slf4j:slf4j-log4j12:1.7.6")
            testImplementation("org.apache.logging.log4j:log4j-api:${project.ext.log4jVersion}")
            testImplementation("org.apache.logging.log4j:log4j-core:${project.ext.log4jVersion}")
            testImplementation("org.apache.logging.log4j:log4j-1.2-api:${project.ext.log4jVersion}")
            testImplementation("net.java.dev.jna:jna:4.2.2")
            testImplementation("org.codehaus.groovy:groovy:${project.ext.groovyVersion}:indy")
            testImplementation("org.locationtech.spatial4j:spatial4j:0.6")
            testImplementation("com.vividsolutions:jts:1.13")

            itestImplementation(project.sourceSets.main.output)
            itestImplementation(project.configurations.testImplementation)
            itestImplementation(project.sourceSets.test.output)
            itestImplementation(project.configurations.testRuntimeClasspath)
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
    }

    /**
     * Configure any common properties/tasks of the Java/IDE/etc build plugins
     * @param project to be configured
     */
    private static void configureBuildTasks(Project project) {
        // Target Java 1.8 compilation
        project.sourceCompatibility = '1.8'
        project.targetCompatibility = '1.8'

        // TODO: Remove all root project distribution logic. It should exist in a separate dist project.
        if (project != project.rootProject) {
            SourceSet mainSourceSet = project.sourceSets.main

            // Add java source to project's source elements and javadoc elements
            FileCollection javaSourceDirs = mainSourceSet.java.sourceDirectories
            javaSourceDirs.each { File srcDir ->
                project.getArtifacts().add('sourceElements', srcDir)
                project.getArtifacts().add('javadocElements', srcDir)
            }

            // Add scala sources to source elements if that plugin is applied
            project.getPlugins().withType(ScalaPlugin.class) {
                FileCollection scalaSourceDirs = mainSourceSet.scala.sourceDirectories
                scalaSourceDirs.each { File scalaSrcDir ->
                    project.getArtifacts().add('sourceElements', scalaSrcDir)
                }
            }
        }

        JavaCompile compileJava = project.tasks.getByName('compileJava') as JavaCompile
        compileJava.getOptions().setCompilerArgs(['-Xlint:unchecked', '-Xlint:options'])

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
        manifest.attributes['Repository-Revision'] = BuildParams.gitRevision
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

        if (project != project.rootProject) {
            project.getArtifacts().add('distElements', jar)
        }

        // Jar up the sources of the project
        Jar sourcesJar = project.tasks.create('sourcesJar', Jar)
        sourcesJar.dependsOn(project.tasks.classes)
        sourcesJar.classifier = 'sources'
        sourcesJar.from(project.sourceSets.main.allSource)
        // TODO: Remove when root project does not handle distribution
        if (project != project.rootProject) {
            sourcesJar.from(project.configurations.additionalSources)
            project.getArtifacts().add('distElements', sourcesJar)
        }

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
        // TODO: Remove when root project does not handle distribution
        if (project != project.rootProject) {
            javadoc.source += project.files(project.configurations.javadocSources)
        }
        // Set javadoc executable to runtime Java (1.8)
        javadoc.executable = new File(project.ext.runtimeJavaHome, 'bin/javadoc')

        MinimalJavadocOptions javadocOptions = javadoc.getOptions()
        javadocOptions.docFilesSubDirs = true
        javadocOptions.outputLevel = JavadocOutputLevel.QUIET
        javadocOptions.breakIterator = true
        javadocOptions.author = false
        javadocOptions.header = project.name
        javadocOptions.showFromProtected()
        javadocOptions.addStringOption('Xdoclint:none', '-quiet')
        javadocOptions.groups = [
                'Elasticsearch Map/Reduce' : ['org.elasticsearch.hadoop.mr*'],
                'Elasticsearch Hive' : ['org.elasticsearch.hadoop.hive*'],
                'Elasticsearch Pig' : ['org.elasticsearch.hadoop.pig*'],
                'Elasticsearch Spark' : ['org.elasticsearch.spark*'],
                'Elasticsearch Storm' : ['org.elasticsearch.storm*'],
        ]
        javadocOptions.links = [ // External doc links
                "https://docs.oracle.com/javase/8/docs/api/",
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
        if (project != project.rootProject) {
            project.getArtifacts().add('distElements', javadocJar)
        }

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
                        name = 'Elastic'
                        url = 'https://www.elastic.co'
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
        if (project != project.rootProject) {
            TaskProvider<Task> itestJar = project.tasks.register('itestJar', Jar) { Jar itestJar ->
                itestJar.dependsOn(project.tasks.getByName('jar'))
                itestJar.getArchiveClassifier().set('testing')

                // Add this project's classes to the testing uber-jar
                itestJar.from(project.sourceSets.main.output)
                itestJar.from(project.sourceSets.test.output)
                itestJar.from(project.sourceSets.itest.output)
            }

            Test integrationTest = project.tasks.create('integrationTest', StandaloneRestIntegTestTask.class)
            integrationTest.dependsOn(itestJar)

            itestJar.configure { Jar jar ->
                integrationTest.doFirst {
                    integrationTest.systemProperty("es.hadoop.job.jar", jar.getArchiveFile().get().asFile.absolutePath)
                }
            }

            integrationTest.testClassesDirs = project.sourceSets.itest.output.classesDirs
            integrationTest.classpath = project.sourceSets.itest.runtimeClasspath
            integrationTest.excludes = ["**/Abstract*.class"]

            integrationTest.ignoreFailures = false

            integrationTest.executable = "${project.ext.get('runtimeJavaHome')}/bin/java"
            integrationTest.minHeapSize = "256m"
            integrationTest.maxHeapSize = "2g"

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
            project.logger.info "Configuring ${project.name} integrationTest task to use ES Fixture"
            // Create the cluster fixture around the integration test.
            // There's probably a more elegant way to do this in Gradle
            project.plugins.apply("es.hadoop.cluster")
        }
    }

    private static void configurePrecommit(Project project) {
        List<Object> precommitTasks = []
        LicenseHeadersTask licenseHeaders = project.tasks.create('licenseHeaders', LicenseHeadersTask.class)
        precommitTasks.add(licenseHeaders)

        if (!project.path.startsWith(":qa")) {
            TaskProvider<DependencyLicensesTask> dependencyLicenses = project.tasks.register('dependencyLicenses', DependencyLicensesTask.class) {
                dependencies = project.configurations.runtimeClasspath.fileCollection {
                    !(it instanceof ProjectDependency)
                }
                mapping from: /hadoop-.*/, to: 'hadoop'
                mapping from: /hive-.*/, to: 'hive'
                mapping from: /jackson-.*/, to: 'jackson'
                mapping from: /spark-.*/, to: 'spark'
                mapping from: /scala-.*/, to: 'scala'
            }
            // we also create the updateShas helper task that is associated with dependencyLicenses
            UpdateShasTask updateShas = project.tasks.create('updateShas', UpdateShasTask.class)
            updateShas.parentTask = dependencyLicenses

            precommitTasks.add(dependencyLicenses)
        }
        Task precommit = project.tasks.create('precommit')
        precommit.dependsOn(precommitTasks)
        project.tasks.getByName('check').dependsOn(precommit)
    }

    private static void configureDependenciesInfo(Project project) {
        if (!project.path.startsWith(":qa")) {
            project.getPluginManager().apply(DependenciesInfoPlugin.class)
        }
    }
}
