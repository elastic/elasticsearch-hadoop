package org.elasticsearch.hadoop.gradle.scala


import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.internal.DefaultDomainObjectSet
import org.gradle.api.plugins.scala.ScalaBasePlugin
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.internal.os.OperatingSystem

class ScalaVariantPlugin implements Plugin<Project> {

    private static final String NESTED_BUILD_RUN = 'scala.variant'

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

            // For all variants make a crossBuild#variant per variant version
            variantExtension.variants.all { String variantVersion ->
                String variantBaseVersion = baseVersionFromFullVersion(variantVersion)
                Exec crossBuildForVariant = project.getTasks().create("variants#${variantBaseVersion.replace('.', '_')}", Exec.class)

                // The crossBuild runs the distribution task with a different scala property, and 'nestedRun' set to true
                Map<String, String> properties = new HashMap<>()
                properties.put(NESTED_BUILD_RUN, variantVersion)
                properties.put('shush', 'true')
                if (project.properties.containsKey("localRepo")) {
                    properties.put('localRepo', 'true')
                }

                if (OperatingSystem.current().isWindows()) {
                    crossBuildForVariant.executable('gradlew.bat')
                } else {
                    crossBuildForVariant.executable('./gradlew')
                }

                crossBuildForVariant.args(distribution.getPath())
                crossBuildForVariant.args(properties.collect { key, val -> "-P${key}=${val}" })
                crossBuildForVariant.args('-S')
                if (project.logger.isDebugEnabled()) {
                    crossBuildForVariant.args('--debug')
                } else if (project.logger.isInfoEnabled()) {
                    crossBuildForVariant.args('--info')
                }
                crossBuildForVariant.workingDir(project.rootDir)

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
            project.sourceSets.each {
                it.java.outputDir = project.file(it.java.outputDir.absolutePath.replaceAll("classes", "classes.${variantSuffix}"))
                it.scala.outputDir = project.file(it.scala.outputDir.absolutePath.replaceAll("classes", "classes.${variantSuffix}"))
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
