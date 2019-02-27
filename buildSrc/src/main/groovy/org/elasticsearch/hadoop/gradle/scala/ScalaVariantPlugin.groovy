package org.elasticsearch.hadoop.gradle.scala

import org.gradle.StartParameter
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.FileCollection
import org.gradle.api.internal.DefaultDomainObjectSet
import org.gradle.api.plugins.scala.ScalaBasePlugin
import org.gradle.api.tasks.GradleBuild
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test

class ScalaVariantPlugin implements Plugin<Project> {

    private static final String NESTED_BUILD_RUN = 'scala.variant'

    // Gradle 5.0 does some sort of locking on the main build script when performing builds. Since our variant compile
    // tasks are based on launching sub-builds of Gradle, in some cases Gradle tries to pipeline and mix the tasks
    // together. This leads to multiple tasks hitting some deep locking code and instead of waiting, fail fast. This
    // mechanism ensures that no variant build tasks are pipelined with any other variant builds.
    private static final List<Task> variantTasks = new ArrayList<Task>()

    @Override
    void apply(Project project) {
        // Ensure plugins
        project.getPluginManager().apply(ScalaBasePlugin.class)

        // Make a container for scala versions
        ScalaVariantExtension variantExtension = project.getExtensions().create('variants', ScalaVariantExtension.class, project)

        // Add a lifecycle task called crossBuild
        Task crossBuild = project.getTasks().create('variants')

        // Current build is not a nested build:
        if (isRegularBuild(project)) {
            // Wire variants into the build
            Task distribution = project.getTasks().getByName('distribution')
            distribution.dependsOn(crossBuild)

            // Ensure that all project cross-building happens after the standard Jar task.
            Task jar = project.getTasks().getByName('jar')
            crossBuild.dependsOn(jar)

            // For all variants make a crossBuild#variant per variant version
            variantExtension.variants.all { String variantVersion ->
                String variantBaseVersion = baseVersionFromFullVersion(variantVersion)
                GradleBuild crossBuildForVariant = project.getTasks().create("variants#${variantBaseVersion.replace('.', '_')}", GradleBuild.class)

                // Ensure that no variant build tasks are pipelined with any other variant builds.
                // We lock explicitly here to ensure no mutations between checking and setting.
                synchronized (variantTasks) {
                    if (!variantTasks.isEmpty()) {
                        crossBuildForVariant.mustRunAfter(variantTasks.last())
                    }
                    variantTasks.add(crossBuildForVariant)
                }

                // The crossBuild runs the distribution task with a different scala property, and 'nestedRun' set to true
                Map<String, String> properties = new HashMap<>()
                properties.put(NESTED_BUILD_RUN, variantVersion)
                properties.put('shush', 'true')
                if (project.properties.containsKey("localRepo")) {
                    properties.put('localRepo', 'true')
                }

                StartParameter parameters = project.gradle.startParameter.newBuild()
                parameters.setProjectProperties(properties)

                crossBuildForVariant.setStartParameter(parameters)
                crossBuildForVariant.setTasks([distribution.getPath()])

                // The crossBuild depends on each variant build
                crossBuild.dependsOn(crossBuildForVariant)
            }
        }

        // Sense if we're doing a nested run. If so, use the variant version instead of the extension's default version
        // for the version property.
        if (isNestedRun(project)) {
            String configuredVersion = project.getProperties().get(NESTED_BUILD_RUN).toString()

            project.logger.info("Cross-Building scala variant of [$configuredVersion]. " +
                    "Ignoring default version...")
            variantExtension.setDefaultVersion(configuredVersion)

            // The crossBuild is disabled if we're doing a 'nestedRun'
            crossBuild.setEnabled(false)

            String variantSuffix = (project.ext.scalaMajorVersion as String).replace('.', '')

            // When working with a variant use a different folder to cache the artifacts between builds
            project.sourceSets.each { SourceSet sourceSet ->
                sourceSet.java.outputDir = project.file(sourceSet.java.outputDir.absolutePath.replaceAll("classes", "classes.${variantSuffix}"))
                sourceSet.scala.outputDir = project.file(sourceSet.scala.outputDir.absolutePath.replaceAll("classes", "classes.${variantSuffix}"))
            }

            Javadoc javadoc = project.getTasks().getByName('javadoc') as Javadoc
            javadoc.setDestinationDir(project.file("${project.docsDir}/javadoc-${variantSuffix}"))

            Test integrationTest = project.getTasks().getByName('integrationTest') as Test
            integrationTest.setTestClassesDirs(project.sourceSets.itest.output.classesDirs)
        }
    }

    static boolean isRegularBuild(Project project) {
        return !isNestedRun(project)
    }

    static boolean isNestedRun(Project project) {
        return project.hasProperty(NESTED_BUILD_RUN)
    }

    static class ScalaVariantExtension {
        private final Project project
        protected DefaultDomainObjectSet<String> variants = new DefaultDomainObjectSet<>(String.class)
        protected String defaultVersion
        protected String defaultBaseVersion

        ScalaVariantExtension(Project project) {
            this.project = project
            this.defaultVersion = null
        }

        void setTargetVersions(String... versions) {
            List<String> toAdd = versions as List
            if (defaultBaseVersion != null) {
                toAdd.removeAll { (baseVersionFromFullVersion(it) == defaultBaseVersion) }
            }
            variants.addAll(toAdd)
        }

        void targetVersions(String... versions) {
            setTargetVersions(versions)
        }

        void setDefaultVersion(String version) {
            if (defaultVersion != null) {
                // Ignore new values after being set.
                project.logger.warn("Ignoring default version of [$version] as it is already configured as [$defaultVersion]")
                return
            }

            // Set the version string
            defaultVersion = version
            defaultBaseVersion = baseVersionFromFullVersion(version)

            // Remove any versions from variants that match
            variants.removeAll { (baseVersionFromFullVersion(it) == defaultBaseVersion) }

            // Configure project properties to contain the scala versions
            project.ext.scalaVersion = defaultVersion
            project.ext.scalaMajorVersion = defaultBaseVersion

            // Set the major version on the archives base name.
            project.archivesBaseName += "_${project.ext.scalaMajorVersion}"
        }

        void defaultVersion(String version) {
            setDefaultVersion(version)
        }
    }

    /**
     * Takes an epoch.major.minor version and returns the epoch.major version form of it.
     * @return
     */
    static String baseVersionFromFullVersion(String fullVersion) {
        List<String> versionParts = fullVersion.tokenize('.')
        if (versionParts.size() != 3) {
            throw new GradleException("Invalid Scala Version - Version [$fullVersion] is not a full scala version (epoch.major.minor).")
        }
        return versionParts.init().join('.')
    }

}
