package org.elasticsearch.hadoop.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ExternalDependency
import org.gradle.api.file.CopySpec
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.bundling.Zip
import org.gradle.api.tasks.javadoc.Javadoc

/**
 * Plugin for configuring any project module that ends up packaged as part of the
 * master distribution (all integrations combined).
 * <p>
 * Note: This is only for defining settings/tasks that have a root/subproject relationship that have to
 * do with packaging. Tasks or settings that are subject to ALL projects (not just the ones that are shipped
 * in the master project archive) should be configured in {@link BuildPlugin}.
 *
 * @see RootBuildPlugin
 */
class IntegrationBuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project target) {
        // Ensure build commands/settings/tasks are on the root project before starting.
        target.getRootProject().getPluginManager().apply(RootBuildPlugin.class)

        // Ensure that the common build plugin is already applied
        target.getPluginManager().apply(BuildPlugin.class)

        configureProjectJars(target)
        configureProjectZip(target)
        configureRootProjectDependencies(target)
    }

    private static def configureProjectJars(Project project) {
        // We do this after evaluation since the scala projects may change around what the final archive name is.
        project.afterEvaluate {
            // Add the sub-project's jar contents to the project's uber-jar
            Jar rootJar = project.rootProject.getTasks().getByName('jar') as Jar
            rootJar.dependsOn(project.tasks.jar)
            rootJar.from(project.zipTree(project.tasks.jar.archivePath)) {
                exclude "META-INF/*"
                include "META-INF/services"
                include "**/*"
            }

            // Add sources to root project's sources jar
            Jar rootSourcesJar = project.rootProject.getTasks().getByName("sourcesJar") as Jar
            rootSourcesJar.from(project.sourceSets.main.allJava.srcDirs)

            // Configure root javadoc process to compile and consume this project's javadocs
            Javadoc rootJavadoc = project.rootProject.getTasks().getByName("javadoc") as Javadoc
            Javadoc subJavadoc = project.getTasks().getByName('javadoc') as Javadoc
            rootJavadoc.setSource(subJavadoc.source)
            rootJavadoc.classpath += project.files(project.sourceSets.main.compileClasspath)
        }
    }

    /**
     * Add this module's jar output to the root project's master zip distribution.
     * @param project to be configured
     */
    private static void configureProjectZip(Project project) {
        // We do this after evaluation since the scala projects may change around what the final archive name is.
        project.afterEvaluate {
            Zip rootDistZip = project.rootProject.getTasks().getByName('distZip') as Zip
            rootDistZip.dependsOn(project.getTasks().pack)

            project.getTasks().withType(Jar.class).each { Jar jarTask ->
                // Add jar output under the dist directory
                rootDistZip.from(jarTask.archivePath) { CopySpec copySpecification ->
                    copySpecification.into("${project.rootProject.ext.folderName}/dist")
                    copySpecification.setDuplicatesStrategy(DuplicatesStrategy.WARN)
                }
            }
        }
    }

    /**
     * For all the dependencies set on this project, set them on the root project as well.
     * @param project to be configured
     */
    private static void configureRootProjectDependencies(Project project) {
        // We do this in an after evaluate so that we pick up all dependencies set after the plugin was configured.
        // If this becomes a problem, we could see if there's a way to listen for new dependencies and add them
        // to root at the same time.
        project.afterEvaluate {
            project.getConfigurations().getByName('compile').getAllDependencies()
                    .withType(ExternalDependency.class)
                    .each { Dependency dependency ->
                    // Convert the scope to optional on the root project - it will have every integration in it, and
                    // users may not need every dependency (except hadoop and jackson)
                    String scope = (dependency.group in ['org.apache.hadoop', 'org.codehaus.jackson'] ? 'provided' : 'optional')
                    project.rootProject.getDependencies().add(scope, dependency)
                }

            project.getConfigurations().getByName('provided').getAllDependencies()
                .withType(ExternalDependency.class)
                .each { Dependency dependency ->
                    // Convert the scope to optional on the root project - it will have every integration in it, and
                    // users may not need every dependency (except hadoop and jackson)
                    String scope = (dependency.group in ['org.apache.hadoop', 'org.codehaus.jackson'] ? 'provided' : 'optional')
                    project.rootProject.getDependencies().add(scope, dependency)
                }

            project.getConfigurations().getByName('optional').getAllDependencies()
                .withType(ExternalDependency.class)
                .each { Dependency dependency ->
                    // Convert the scope to optional on the root project - it will have every integration in it, and
                    // users may not need every dependency (except hadoop and jackson)
                    String scope = (dependency.group in ['org.apache.hadoop', 'org.codehaus.jackson'] ? 'provided' : 'optional')
                    project.rootProject.getDependencies().add(scope, dependency)
                }
        }
    }
}
