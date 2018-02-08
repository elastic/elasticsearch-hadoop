package org.elasticsearch.hadoop.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * Common Build Settings for the ES-Hadoop project.
 */
class BuildSettingsPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        greet(project)
        configureVersions(project)
    }

    // Just a bit of fun for the moment
    private static void greet(Project project) {
        if (!project.rootProject.hasProperty('versionsConfigured')) {
            println '======================================='
            println 'Elasticsearch Build Hamster says Hello!'
            println '======================================='
        }
    }

    private void configureVersions(Project project) {
        if (!project.rootProject.ext.has('versionsConfigured')) {
            project.rootProject.version = VersionProperties.ESHADOOP_VERSION
            project.rootProject.ext.eshadoopVersion = VersionProperties.ESHADOOP_VERSION
            project.rootProject.ext.elasticsearchVersion = VersionProperties.ELASTICSEARCH_VERSION
            project.rootProject.ext.luceneVersion = org.elasticsearch.gradle.VersionProperties.lucene
            project.rootProject.ext.versions = VersionProperties.VERSIONS
            project.rootProject.ext.versionsConfigured = true
        }
        project.ext.eshadoopVersion = project.rootProject.ext.eshadoopVersion
        project.ext.elasticsearchVersion = project.rootProject.ext.elasticsearchVersion
        project.ext.versions = project.rootProject.ext.versions
        project.version = project.rootProject.version
    }
}
