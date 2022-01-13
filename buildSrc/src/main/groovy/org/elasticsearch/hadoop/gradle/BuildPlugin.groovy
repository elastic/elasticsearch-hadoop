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

package org.elasticsearch.hadoop.gradle

import org.elasticsearch.hadoop.gradle.buildtools.DependenciesInfoPlugin
import org.elasticsearch.hadoop.gradle.buildtools.DependencyLicensesTask
import org.elasticsearch.hadoop.gradle.buildtools.LicenseHeadersTask
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask
import org.elasticsearch.hadoop.gradle.buildtools.UpdateShasTask
import org.elasticsearch.hadoop.gradle.buildtools.info.BuildParams
import org.elasticsearch.hadoop.gradle.scala.SparkVariantPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.XmlProvider
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.DependencyResolveDetails
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.ResolutionStrategy
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.component.SoftwareComponentFactory
import org.gradle.api.file.CopySpec
import org.gradle.api.file.FileCollection
import org.gradle.api.java.archives.Manifest
import org.gradle.api.plugins.JavaLibraryPlugin
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.scala.ScalaPlugin
import org.gradle.api.provider.Provider
import org.gradle.api.publish.maven.MavenPom
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.tasks.GenerateMavenPom
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.external.javadoc.JavadocOutputLevel
import org.gradle.external.javadoc.MinimalJavadocOptions
import org.gradle.plugins.ide.eclipse.EclipsePlugin
import org.gradle.plugins.ide.idea.IdeaPlugin
import org.w3c.dom.NodeList

import javax.inject.Inject

import static org.elasticsearch.hadoop.gradle.scala.SparkVariantPlugin.SparkVariantPluginExtension
import static org.elasticsearch.hadoop.gradle.scala.SparkVariantPlugin.SparkVariant

class BuildPlugin implements Plugin<Project>  {

    public static final String SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME = "sharedTestImplementation"
    public static final String SHARED_ITEST_IMPLEMENTATION_CONFIGURATION_NAME = "sharedItestImplementation"

    private final SoftwareComponentFactory softwareComponentFactory

    @Inject
    BuildPlugin(SoftwareComponentFactory softwareComponentFactory) {
        this.softwareComponentFactory = softwareComponentFactory
    }

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
    }

    /** Return the configuration name used for finding transitive deps of the given dependency. */
    private static String transitiveDepConfigName(String groupId, String artifactId, String version) {
        return "_transitive_${groupId}_${artifactId}_${version}"
    }

    /**
     * Applies a closure to all dependencies in a configuration (currently or in the future) that disables the
     * resolution of transitive dependencies except for projects in the group <code>org.elasticsearch</code>.
     * @param configuration to disable transitive dependencies on
     */
    static void disableTransitiveDependencies(Project project, Configuration configuration) {
        configuration.dependencies.all { Dependency dep ->
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
    }

    private static Configuration createConfiguration(Project project, String configurationName, boolean canBeConsumed, boolean canBeResolved,
                                            String usageAttribute) {
        return createConfiguration(project, configurationName, canBeConsumed, canBeResolved, usageAttribute, null)
    }

    private static Configuration createConfiguration(Project project, String configurationName, boolean canBeConsumed, boolean canBeResolved,
                                            String usageAttribute, String libraryElements) {
        Configuration configuration = project.configurations.create(configurationName)
        configuration.canBeConsumed = canBeConsumed
        configuration.canBeResolved = canBeResolved
        configuration.attributes {
            // Changing USAGE is required when working with Scala projects, otherwise the source dirs get pulled
            // into incremental compilation analysis.
            attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage, usageAttribute))
            if (libraryElements != null) {
                attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, project.objects.named(LibraryElements, libraryElements))
            }
        }
        return configuration
    }

    private static void configureConfigurations(Project project) {
        // Create a configuration that will hold common test dependencies to be shared with all of a project's test sources, including variants if present
        Configuration sharedTestImplementation = project.configurations.create(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME)
        project.configurations.getByName(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME).extendsFrom(sharedTestImplementation)
        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            sparkVariants.featureVariants { SparkVariant variant ->
                Configuration variantTestImplementation = project.configurations.getByName(variant.configuration(SourceSet.TEST_SOURCE_SET_NAME, JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME))
                variantTestImplementation.extendsFrom(sharedTestImplementation)
            }
        }

        if (project != project.rootProject) {
            // Set up avenues for sharing source files between projects in order to create embedded Javadocs
            // Import source configuration
            Configuration additionalSources = createConfiguration(project, 'additionalSources', false, true, 'java-source', 'sources')

            // Export source configuration - different from 'sourcesElements' which contains sourceJars instead of source files
            Configuration sourceElements = createConfiguration(project, 'sourceElements', true, false, 'java-source', 'sources')
            sourceElements.extendsFrom(additionalSources)

            // Import javadoc sources
            createConfiguration(project, 'javadocSources', false, true, 'javadoc-source', 'sources')

            // Export javadoc source configuration - different from 'javadocElements' which contains javadocJars instead of java source files used to generate javadocs
            Configuration javadocSourceElements = createConfiguration(project, 'javadocSourceElements', true, false, 'javadoc-source', 'sources')
            javadocSourceElements.extendsFrom(additionalSources)

            // Export configuration for archives that should be in the distribution
            // TODO: Should we ditch this in favor of just using the built in exporting configurations? all three artifact types have them now
            createConfiguration(project, 'distElements', true, false, 'packaging')

            // Do the same for any variants if the project has them
            project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
                SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
                sparkVariants.featureVariants { SparkVariant variant ->
                    Configuration vAdditionalSources = createConfiguration(project, variant.configuration('additionalSources'), false, true, 'java-source', 'sources')

                    Configuration vSourceElements = createConfiguration(project, variant.configuration('sourceElements'), true, false, 'java-source', 'sources')
                    vSourceElements.extendsFrom(vAdditionalSources)

                    createConfiguration(project, variant.configuration('javadocSources'), false, true, 'javadoc-source', 'sources')

                    Configuration vJavadocSourceElements = createConfiguration(project, variant.configuration('javadocSourceElements'), true, false, 'javadoc-source', 'sources')
                    vJavadocSourceElements.extendsFrom(vAdditionalSources)

                    createConfiguration(project, variant.configuration('distElements'), true, false, 'packaging')
                }
                sparkVariants.all { SparkVariant variant ->
                    // Set capabilities on ALL variants if variants are enabled.
                    // These are required to differentiate the different producing configurations from each other when resolving artifacts for consuming configurations.
                    String variantCapability = variant.getCapabilityName(project.getVersion())
                    project.configurations.getByName(variant.configuration('sourceElements')).getOutgoing().capability(variantCapability)
                    project.configurations.getByName(variant.configuration('javadocSourceElements')).getOutgoing().capability(variantCapability)
                    project.configurations.getByName(variant.configuration('distElements')).getOutgoing().capability(variantCapability)
                }
            }
        }

        if (project.path.startsWith(":qa")) {
            return
        }

        // force all dependencies added directly to compile/testCompile to be non-transitive, except for Elasticsearch projects
        disableTransitiveDependencies(project, project.configurations.api)
        disableTransitiveDependencies(project, project.configurations.implementation)
        disableTransitiveDependencies(project, project.configurations.compileOnly)
        disableTransitiveDependencies(project, project.configurations.runtimeOnly)

        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            sparkVariants.featureVariants { SparkVariant variant ->
                disableTransitiveDependencies(project, project.getConfigurations().findByName(variant.configuration("api")))
                disableTransitiveDependencies(project, project.getConfigurations().findByName(variant.configuration("implementation")))
                disableTransitiveDependencies(project, project.getConfigurations().findByName(variant.configuration("compileOnly")))
                disableTransitiveDependencies(project, project.getConfigurations().findByName(variant.configuration("runtimeOnly")))
            }
        }
    }

    /**
     * Add all common build dependencies to the project, and provide any common resolution strategies
     * @param project to be configured
     */
    private static void configureDependencies(Project project) {
        SourceSetContainer sourceSets = project.sourceSets as SourceSetContainer
        SourceSet main = sourceSets.getByName('main')

        // Create an itest source set, just like the test source set
        SourceSet itest = sourceSets.create('itest')
        itest.setCompileClasspath(project.objects.fileCollection().from(main.getOutput(), project.getConfigurations().getByName('itestCompileClasspath')))
        itest.setRuntimeClasspath(project.objects.fileCollection().from(itest.getOutput(), main.getOutput(), project.getConfigurations().getByName('itestRuntimeClasspath')))

        // Set configuration extension for itest:
        //   shared test <-- shared itest <-- itest
        //   test <-- itest
        Configuration sharedItestImplementation = project.configurations.create(SHARED_ITEST_IMPLEMENTATION_CONFIGURATION_NAME)
        Configuration sharedTestImplementation = project.configurations.getByName(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME)
        Configuration testImplementation = project.configurations.getByName('testImplementation')
        Configuration itestImplementation = project.configurations.getByName('itestImplementation')
        sharedItestImplementation.extendsFrom(sharedTestImplementation)
        itestImplementation.extendsFrom(sharedItestImplementation)
        itestImplementation.extendsFrom(testImplementation)

        // Create an itest source set for each variant
        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            sparkVariants.featureVariants { SparkVariant variant ->
                SparkVariantPlugin.configureAdditionalVariantSourceSet(project, variant, 'itest')

                Configuration variantTestImplementation = project.configurations.getByName(variant.configuration('test', 'implementation'))
                Configuration variantITestImplementation = project.configurations.getByName(variant.configuration('itest', 'implementation'))
                variantITestImplementation.extendsFrom(sharedItestImplementation)
                variantITestImplementation.extendsFrom(variantTestImplementation)
            }
        }

        // Detail all common dependencies
        project.dependencies {
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "junit:junit:${project.ext.junitVersion}")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.hamcrest:hamcrest-all:${project.ext.hamcrestVersion}")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "joda-time:joda-time:2.8")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.slf4j:slf4j-log4j12:1.7.6")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.apache.logging.log4j:log4j-api:${project.ext.log4jVersion}")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.apache.logging.log4j:log4j-core:${project.ext.log4jVersion}")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.apache.logging.log4j:log4j-1.2-api:${project.ext.log4jVersion}")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "net.java.dev.jna:jna:4.2.2")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.codehaus.groovy:groovy:${project.ext.groovyVersion}:indy")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.locationtech.spatial4j:spatial4j:0.6")
            add(SHARED_TEST_IMPLEMENTATION_CONFIGURATION_NAME, "com.vividsolutions:jts:1.13")

            // TODO: May not be needed on all itests
            add(SHARED_ITEST_IMPLEMENTATION_CONFIGURATION_NAME, "org.apache.hadoop:hadoop-minikdc:${project.ext.minikdcVersion}") {
                // For some reason, the dependencies that are pulled in with MiniKDC have multiple resource files
                // that cause issues when they are loaded. We exclude the ldap schema data jar to get around this.
                exclude group: "org.apache.directory.api", module: "api-ldap-schema-data"
            }
        }

        // Deal with the messy conflicts out there
        project.configurations.all { Configuration conf ->
            conf.resolutionStrategy { ResolutionStrategy resolve ->
                // Locking on to joda 2.8
                resolve.force('joda-time:joda-time:2.8')

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
                project.getArtifacts().add('javadocSourceElements', srcDir)
            }

            // Add scala sources to source elements if that plugin is applied
            project.getPlugins().withType(ScalaPlugin.class) {
                FileCollection scalaSourceDirs = mainSourceSet.scala.sourceDirectories
                scalaSourceDirs.each { File scalaSrcDir ->
                    project.getArtifacts().add('sourceElements', scalaSrcDir)
                }
            }

            // Do the same for any variants
            project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
                SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
                sparkVariants.featureVariants { SparkVariant variant ->
                    SourceSet variantMainSourceSet = project.sourceSets.getByName(variant.getSourceSetName('main'))

                    FileCollection variantJavaSourceDirs = variantMainSourceSet.java.sourceDirectories
                    variantJavaSourceDirs.each { File srcDir ->
                        project.getArtifacts().add(variant.configuration('sourceElements'), srcDir)
                        project.getArtifacts().add(variant.configuration('javadocSourceElements'), srcDir)
                    }

                    FileCollection variantScalaSourceDirs = variantMainSourceSet.scala.sourceDirectories
                    variantScalaSourceDirs.each { File scalaSrcDir ->
                        project.getArtifacts().add(variant.configuration('sourceElements'), scalaSrcDir)
                    }
                }
            }
        }

        project.tasks.withType(JavaCompile) { JavaCompile compile ->
            compile.getOptions().setCompilerArgs(['-Xlint:unchecked', '-Xlint:options'])
        }

        // Enable HTML test reports
        project.tasks.withType(Test) { Test testTask ->
            testTask.getReports().getByName('html').setRequired(true)
        }

        // Configure project jar task with manifest and include license and notice data.
        project.tasks.withType(Jar) { Jar jar ->
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

            // TODO: Are these better to be set on just the jar or do these make sense to be on all jars (jar, javadoc, source)?
            jar.from("${project.rootDir}/docs/src/info") { CopySpec spec ->
                spec.include("license.txt")
                spec.include("notice.txt")
                spec.into("META-INF")
                spec.expand(copyright: new Date().format('yyyy'), version: project.version)
            }
        }

        if (project != project.rootProject) {
            project.getArtifacts().add('distElements', project.tasks.getByName('jar'))
            project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
                SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
                sparkVariants.featureVariants { SparkVariant variant ->
                    project.getArtifacts().add(variant.configuration('distElements'), project.tasks.getByName(variant.taskName('jar')))
                }
            }
        }

        // Creates jar tasks and producer configurations for javadocs and sources.
        // Producer configurations (javadocElements and sourcesElements) contain javadoc and source JARS. This makes
        // them more akin to distElements than the source code configurations (javadocSourceElements and sourceElements)
        project.java {
            withJavadocJar()
            withSourcesJar()
        }
        Jar sourcesJar = project.tasks.getByName('sourcesJar') as Jar
        sourcesJar.dependsOn(project.tasks.classes)
        // TODO: Remove when root project does not handle distribution
        if (project != project.rootProject) {
            sourcesJar.from(project.configurations.additionalSources)
            project.getArtifacts().add('distElements', sourcesJar)
        }
        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            sparkVariants.featureVariants { SparkVariant variant ->
                // Don't need to create sources jar task since it is already created by the variant plugin
                Jar variantSourcesJar = project.tasks.getByName(variant.taskName('sourcesJar')) as Jar
                variantSourcesJar.dependsOn(project.tasks.getByName(variant.taskName('classes')))
                variantSourcesJar.from(project.configurations.getByName(variant.configuration('additionalSources')))
                project.getArtifacts().add(variant.configuration('distElements'), variantSourcesJar)
            }
        }

        // Configure javadoc
        project.tasks.withType(Javadoc) { Javadoc javadoc ->
            javadoc.title = "${project.rootProject.description} ${project.version} API"
            javadoc.excludes = [
                    "org/elasticsearch/hadoop/mr/compat/**",
                    "org/elasticsearch/hadoop/rest/**",
                    "org/elasticsearch/hadoop/serialization/**",
                    "org/elasticsearch/hadoop/util/**",
                    "org/apache/hadoop/hive/**"
            ]
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
        }
        // TODO: Remove when root project does not handle distribution
        if (project != project.rootProject) {
            Javadoc javadoc = project.tasks.getByName('javadoc') as Javadoc
            javadoc.source(project.configurations.javadocSources)
            project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
                SparkVariantPluginExtension sparkVarients = project.getExtensions().getByType(SparkVariantPluginExtension.class)
                sparkVarients.featureVariants { SparkVariant variant ->
                    Javadoc variantJavadoc = project.tasks.getByName(variant.taskName('javadoc')) as Javadoc
                    variantJavadoc.source(project.configurations.getByName(variant.configuration('javadocSources')))
                }
            }
        }

        // Package up the javadocs into their own jar
        Jar javadocJar = project.tasks.getByName('javadocJar') as Jar
        if (project != project.rootProject) {
            project.getArtifacts().add('distElements', javadocJar)
        }
        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            sparkVariants.featureVariants { SparkVariant variant ->
                Jar variantJavadocJar = project.tasks.getByName(variant.taskName('javadocJar')) as Jar
                project.getArtifacts().add(variant.configuration('distElements'), variantJavadocJar)
            }
        }

        // Task for creating ALL of a project's jars - Like assemble, but this includes the sourcesJar and javadocJar.
        // TODO: Assemble is being configured to make javadoc and sources jars no matter what due to the withX() methods above. Is this even required in that case?
        // The assemble task was previously configured to ignore javadoc and source tasks because they can be time consuming to generate when simply building the project.
        // Probably better to just run them.
        Task pack = project.tasks.create('pack')
        pack.dependsOn(project.tasks.jar)
        pack.dependsOn(project.tasks.javadocJar)
        pack.dependsOn(project.tasks.sourcesJar)
        pack.outputs.files(project.tasks.jar.archivePath, project.tasks.javadocJar.archivePath, project.tasks.sourcesJar.archivePath)
        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            sparkVariants.featureVariants { SparkVariant variant ->
                pack.dependsOn(project.tasks.getByName(variant.taskName('jar')))
                pack.dependsOn(project.tasks.getByName(variant.taskName('javadocJar')))
                pack.dependsOn(project.tasks.getByName(variant.taskName('sourcesJar')))
                pack.outputs.files(
                        project.tasks.getByName(variant.taskName('jar')).archivePath,
                        project.tasks.getByName(variant.taskName('javadocJar')).archivePath,
                        project.tasks.getByName(variant.taskName('sourcesJar')).archivePath
                )
            }
        }

        // The distribution task is like assemble, but packages up a lot of extra jars and performs extra tasks that
        // are mostly used for snapshots and releases.
        Task distribution = project.tasks.create('distribution')
        distribution.dependsOn(pack)
        // Co-locate all build artifacts into distributions subdir for easier build automation
        Copy collectArtifacts = project.tasks.create('collectArtifacts', Copy)
        collectArtifacts.from(project.tasks.jar)
        collectArtifacts.from(project.tasks.javadocJar)
        collectArtifacts.from(project.tasks.sourcesJar)
        collectArtifacts.into("${project.buildDir}/distributions")
        collectArtifacts.dependsOn(pack)
        distribution.dependsOn(collectArtifacts)
        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            sparkVariants.featureVariants { SparkVariant variant ->
                Copy variantCollectArtifacts = project.tasks.create('collectArtifacts' + variant.getName(), Copy)
                variantCollectArtifacts.from(project.tasks.getByName(variant.taskName('jar')))
                variantCollectArtifacts.from(project.tasks.getByName(variant.taskName('javadocJar')))
                variantCollectArtifacts.from(project.tasks.getByName(variant.taskName('sourcesJar')))
                variantCollectArtifacts.into("${project.buildDir}/distributions")
                variantCollectArtifacts.dependsOn(pack)
                distribution.dependsOn(variantCollectArtifacts)
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

    private void configureMaven(Project project) {
        project.getPluginManager().apply("maven-publish")

        // Configure Maven publication
        project.publishing {
            publications {
                main(MavenPublication) {
                    from project.components.java
                    suppressAllPomMetadataWarnings() // We get it. Gradle metadata is better than Maven Poms
                }
            }
            repositories {
                maven {
                    name = 'build'
                    url = "file://${project.buildDir}/repo"
                }
            }
        }

        // Configure Maven Pom
        configurePom(project, project.publishing.publications.main)

        // Disable the publishing tasks since we only need the pom generation tasks.
        // If we are working with a project that has a scala variant (see below), we need to modify the pom's
        // artifact id which the publish task does not like (it fails validation when run).
        project.getTasks().withType(PublishToMavenRepository) { PublishToMavenRepository m ->
            m.enabled = false
        }

        // Configure Scala Variants if present
        project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
            // Publishing gets weird when you introduce variants into the project.
            // By default, when adding a spark/scala variant, its outgoing configurations are added to the main java components.
            // The maven publish plugin will take all these variants, smoosh them and their dependencies together, and create
            // one big pom file full of version conflicts. Since spark variants are mutually exclusive, we need to perform a
            // workaround to materialize multiple poms for the different scala variants.
            // TODO: Should this adhoc component configuration work be done in the SparkVariantPlugin?

            SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
            def javaComponent = project.components.java

            // Main variant needs the least configuration on its own, since it is the default publication created above.
            sparkVariants.defaultVariant { SparkVariant variant ->
                updateVariantPomLocationAndArtifactId(project, project.publishing.publications.main, variant)
            }

            // For each spark variant added, we need to do a few things:
            sparkVariants.featureVariants { SparkVariant variant ->
                // Collect all the outgoing configurations that are compatible with publication
                def variantConfigurationsToExcludeFromMain = [
                        variant.configuration("apiElements"),
                        variant.configuration("runtimeElements"),
                        variant.configuration('javadocElements'),
                        variant.configuration('sourcesElements'),
                        variant.configuration('test', 'apiElements'),
                        variant.configuration('test', 'runtimeElements'),
                        variant.configuration('itest', 'apiElements'),
                        variant.configuration('itest', 'runtimeElements')
                ]

                // Remove each of those outgoing configurations from the default java component.
                // This will keep the default variant from being smooshed together with conflicting artifacts/dependencies.
                variantConfigurationsToExcludeFromMain.each {
                    javaComponent.withVariantsFromConfiguration(project.configurations.getByName(it)) {
                        skip()
                    }
                }

                // Create an adhoc component for the variant
                def variantComponent = softwareComponentFactory.adhoc("${variant.getName()}Component")
                // Add it to the list of components that this project declares
                project.components.add(variantComponent)
                // Register the variant's outgoing configurations for publication
                variantComponent.addVariantsFromConfiguration(project.configurations.getByName(variant.configuration("apiElements"))) {
                    it.mapToMavenScope("compile")
                }
                variantComponent.addVariantsFromConfiguration(project.configurations.getByName(variant.configuration("runtimeElements"))) {
                    it.mapToMavenScope("runtime")
                }
                variantComponent.addVariantsFromConfiguration(project.configurations.getByName(variant.configuration("javadocElements"))) {
                    it.mapToMavenScope("runtime")
                }
                variantComponent.addVariantsFromConfiguration(project.configurations.getByName(variant.configuration("sourcesElements"))) {
                    it.mapToMavenScope("runtime")
                }

                // Create a publication for this adhoc component to create pom generation and publishing tasks
                project.publishing {
                    publications {
                        MavenPublication variantPublication = create(variant.getName(), MavenPublication) {
                            from variantComponent
                            suppressAllPomMetadataWarnings() // We get it. Gradle metadata is better than Maven Poms
                        }
                        configurePom(project, variantPublication)
                        updateVariantPomLocationAndArtifactId(project, variantPublication, variant)
                    }
                }
            }
        }

        // Set the pom generation tasks as required for the distribution task.
        project.tasks.withType(GenerateMavenPom).all { GenerateMavenPom pom ->
            project.getTasks().getByName('distribution').dependsOn(pom)
        }
    }

    private static void configurePom(Project project, MavenPublication publication) {
        // Set the pom's destination to the distribution directory
        project.tasks.withType(GenerateMavenPom).all { GenerateMavenPom pom ->
            if (pom.name == "generatePomFileFor${publication.name.capitalize()}Publication") {
                pom.destination = project.provider({"${project.buildDir}/distributions/${project.archivesBaseName}-${project.getVersion()}.pom"})
            }
        }

        // add all items necessary for publication
        Provider<String> descriptionProvider = project.provider({ project.getDescription() })
        MavenPom pom = publication.getPom()
        pom.name = descriptionProvider
        pom.description = descriptionProvider
        pom.url = 'http://github.com/elastic/elasticsearch-hadoop'
        pom.organization {
            name = 'Elastic'
            url = 'https://www.elastic.co/'
        }
        pom.licenses {
            license {
                name = 'The Apache Software License, Version 2.0'
                url = 'https://www.apache.org/licenses/LICENSE-2.0.txt'
                distribution = 'repo'
            }
        }
        pom.scm {
            url = 'https://github.com/elastic/elasticsearch-hadoop'
            connection = 'scm:git:git://github.com/elastic/elasticsearch-hadoop'
            developerConnection = 'scm:git:git://github.com/elastic/elasticsearch-hadoop'
        }
        pom.developers {
            developer {
                name = 'Elastic'
                url = 'https://www.elastic.co'
            }
        }

        publication.getPom().withXml { XmlProvider xml ->
            // add all items necessary for publication
            Node root = xml.asNode()

            // If we have embedded configuration on the project, remove its dependencies from the dependency nodes
            NodeList dependenciesNode = root.get("dependencies") as NodeList
            Configuration embedded = project.getConfigurations().findByName('embedded')
            if (embedded != null) {
                embedded.getAllDependencies().all { Dependency dependency ->
                    Iterator<Node> dependenciesIterator = dependenciesNode.get(0).children().iterator()
                    while (dependenciesIterator.hasNext()) {
                        Node dependencyNode = dependenciesIterator.next()
                        String artifact = dependencyNode.get("artifactId").text()
                        if (artifact == dependency.getName()) {
                            dependenciesIterator.remove()
                            break
                        }
                    }
                }
            }
        }
    }

    private static void updateVariantPomLocationAndArtifactId(Project project, MavenPublication publication, SparkVariant variant) {
        // Add variant classifier to the pom file name if required
        String classifier = variant.shouldClassifySparkVersion() && variant.isDefaultVariant() == false ? "-${variant.getName()}" : ''
        String filename = "${project.archivesBaseName}_${variant.scalaMajorVersion}-${project.getVersion()}${classifier}"
        // Fix the pom name
        project.tasks.withType(GenerateMavenPom).all { GenerateMavenPom pom ->
            if (pom.name == "generatePomFileFor${publication.name.capitalize()}Publication") {
                pom.destination = project.provider({"${project.buildDir}/distributions/${filename}.pom"})
            }
        }
        // Fix the artifactId. Note: The publishing task does not like this happening. Hence it is disabled.
        publication.getPom().withXml { XmlProvider xml ->
            Node root = xml.asNode()
            Node artifactId = (root.get('artifactId') as NodeList).get(0) as Node
            artifactId.setValue("${project.archivesBaseName}_${variant.scalaMajorVersion}")
        }
    }

    /**
     * Create a task specifically for integration tests, add the integration test code to the testing uber-jar,
     * and configure a local Elasticsearch node for use as a test fixture.
     * @param project to be configured
     */
    private static void configureIntegrationTestTask(Project project) {
        if (project != project.rootProject) {
            SourceSetContainer sourceSets = project.sourceSets
            SourceSet mainSourceSet = sourceSets.main
            SourceSet itestSourceSet = sourceSets.itest
            String itestJarTaskName = 'itestJar'
            String jarTaskName = 'jar'
            String itestJarClassifier = 'testing'
            String itestTaskName = 'integrationTest'

            createItestTask(project, mainSourceSet, itestSourceSet, itestJarTaskName, jarTaskName, itestJarClassifier, itestTaskName)
            project.getPlugins().withType(SparkVariantPlugin).whenPluginAdded {
                SparkVariantPluginExtension sparkVariants = project.getExtensions().getByType(SparkVariantPluginExtension.class)
                sparkVariants.featureVariants { SparkVariant variant ->
                    createItestTask(project,
                            sourceSets.getByName(variant.getSourceSetName('main')),
                            sourceSets.getByName(variant.getSourceSetName('itest')),
                            variant.taskName(itestJarTaskName),
                            variant.taskName(jarTaskName),
                            variant.getName() + "-" + itestJarClassifier,
                            variant.itestTaskName()
                    )
                }
            }

            // Only add cluster settings if it's not the root project
            project.logger.info "Configuring ${project.name} integrationTest task to use ES Fixture"
            // Create the cluster fixture around the integration test.
            // There's probably a more elegant way to do this in Gradle
            project.plugins.apply("es.hadoop.cluster")
        }
    }

    private static Test createItestTask(Project project, SourceSet mainSourceSet, SourceSet itestSourceSet,
                                        String itestJarTaskName, String jarTaskName, String itestJarClassifier,
                                        String itestTaskName) {
        TaskProvider<Task> itestJar = project.tasks.register(itestJarTaskName, Jar) { Jar itestJar ->
            itestJar.dependsOn(project.tasks.getByName(jarTaskName))
            itestJar.getArchiveClassifier().set(itestJarClassifier)

            // Add this project's classes to the testing uber-jar
            itestJar.from(mainSourceSet.output)
            itestJar.from(itestSourceSet.output)
        }

        Test integrationTest = project.tasks.create(itestTaskName, StandaloneRestIntegTestTask.class)

        itestJar.configure { Jar jar ->
            integrationTest.doFirst {
                integrationTest.systemProperty("es.hadoop.job.jar", jar.getArchiveFile().get().asFile.absolutePath)
            }
        }

        integrationTest.dependsOn(itestJar)
        integrationTest.testClassesDirs = itestSourceSet.output.classesDirs
        integrationTest.classpath = itestSourceSet.runtimeClasspath
        commonItestTaskConfiguration(project, integrationTest)
        // TODO: Should this be the case? It is in Elasticsearch, but we may have to update some CI jobs?
        project.tasks.check.dependsOn(integrationTest)

        Configuration itestJarConfig = project.getConfigurations().maybeCreate("itestJarConfig")
        itestJarConfig.canBeConsumed = Boolean.TRUE
        itestJarConfig.canBeResolved = Boolean.FALSE
        project.getArtifacts().add(itestJarConfig.getName(), itestJar)

        return integrationTest
    }

    private static void commonItestTaskConfiguration(Project project, Test integrationTest) {
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

        integrationTest.reports.html.required = false
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
