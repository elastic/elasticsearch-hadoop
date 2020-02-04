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

package org.elasticsearch.hadoop.gradle.scala;

import java.util.Objects;

import org.codehaus.groovy.runtime.StringGroovyMethods;

public class SparkScalaVariant {
    private final boolean main;

    private final String variant;
    private final String sparkVersion;
    private final String scalaVersion;
    private final String artifactConfiguration;

    private final String testBase;
    private final String scalaMajorVersion;

    public static SparkScalaVariant main(String artifactConfiguration, String sparkVersion, String scalaVersion) {
        return new SparkScalaVariant(true, "main", sparkVersion, scalaVersion, artifactConfiguration, "test",
                scalaVersion.substring(0, scalaVersion.lastIndexOf('.')));
    }

    public static SparkScalaVariant variant(String variant, String sparkVersion, String scalaVersion) {
        return new SparkScalaVariant(false, variant, sparkVersion, scalaVersion, variant,
                "test" + Character.toUpperCase(variant.charAt(0)) + variant.substring(1),
                scalaVersion.substring(0, scalaVersion.lastIndexOf('.'))
        );
    }

    private SparkScalaVariant(boolean main, String variant, String sparkVersion, String scalaVersion, String artifactConfiguration,
                              String testBase, String scalaMajorVersion) {
        this.main = main;
        this.variant = variant;
        this.sparkVersion = sparkVersion;
        this.scalaVersion = scalaVersion;
        this.artifactConfiguration = artifactConfiguration;
        this.testBase = testBase;
        this.scalaMajorVersion = scalaMajorVersion;
    }

    public boolean isMain() {
        return main;
    }

    public boolean isVariant() {
        return !main;
    }

    public String getVariant() {
        return variant;
    }

    public String getTestVariant() {
        return testBase;
    }

    public String getVariantSourceSetName() {
        return variant;
    }

    public String getTestSourceSetName() {
        return testBase;
    }

    public String getSparkVersion() {
        return sparkVersion;
    }

    public String getScalaVersion() {
        return scalaVersion;
    }

    public String getScalaMajorVersion() {
        return scalaMajorVersion;
    }

    public String getArtifactConfiguration() {
        return artifactConfiguration;
    }

    public String configuration(CharSequence configurationName) {
        return main ? configurationName.toString() : variant + StringGroovyMethods.capitalize(configurationName);
    }

    public String testConfiguration(CharSequence configurationName) {
        return testBase + StringGroovyMethods.capitalize(configurationName);
    }

    public String getTestTaskName() {
        return testBase;
    }

    public String variantTaskName(CharSequence taskName) {
        return main ? taskName.toString() : variant + StringGroovyMethods.capitalize(taskName);
    }

    public String getJarTask() {
        return variantTaskName("jar");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparkScalaVariant that = (SparkScalaVariant) o;
        return Objects.equals(variant, that.variant) &&
                Objects.equals(sparkVersion, that.sparkVersion) &&
                Objects.equals(scalaVersion, that.scalaVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variant, sparkVersion, scalaVersion);
    }

    @Override
    public String toString() {
        return "SparkScalaVariant{" +
                "variant='" + variant + '\'' +
                ", sparkVersion='" + sparkVersion + '\'' +
                ", scalaVersion='" + scalaVersion + '\'' +
                '}';
    }
}
