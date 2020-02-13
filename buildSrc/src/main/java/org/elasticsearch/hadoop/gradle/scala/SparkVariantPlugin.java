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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import groovy.lang.Closure;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.codehaus.groovy.runtime.StringGroovyMethods;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectSet;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.ComponentMetadataContext;
import org.gradle.api.artifacts.ComponentMetadataRule;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.internal.artifacts.repositories.resolver.ComponentMetadataDetailsAdapter;
import org.gradle.api.internal.tasks.DefaultScalaSourceSet;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.Convention;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.scala.ScalaDoc;
import org.gradle.api.tasks.testing.Test;
import org.gradle.util.ConfigureUtil;

import static org.gradle.api.plugins.JavaBasePlugin.DOCUMENTATION_GROUP;
import static org.gradle.api.plugins.JavaBasePlugin.VERIFICATION_GROUP;
import static org.gradle.api.plugins.JavaPlugin.API_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.COMPILE_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.JAR_TASK_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.TEST_TASK_NAME;
import static org.gradle.api.plugins.scala.ScalaPlugin.SCALA_DOC_TASK_NAME;
import static org.gradle.api.tasks.SourceSet.MAIN_SOURCE_SET_NAME;
import static org.gradle.api.tasks.SourceSet.TEST_SOURCE_SET_NAME;

public class SparkVariantPlugin implements Plugin<Project> {

    public static class SparkVariant {

        private final CharSequence name;
        private final boolean isDefaultVariant;
        private final String sparkVersion;
        private final String scalaVersion;
        private final String scalaMajorVersion;
        private final String capability;

        public SparkVariant(String name) {
            throw new GradleException("Cannot create variant named [" + name + "]. Do not instantiate objects directly. " +
                    "You must configure this via the SparkVariantPluginExtension.");
        }

        public SparkVariant(CharSequence name, boolean isDefaultVariant, String sparkVersion, String scalaVersion, String capability) {
            this.name = name;
            this.isDefaultVariant = isDefaultVariant;
            this.sparkVersion = sparkVersion;
            this.scalaVersion = scalaVersion;
            this.scalaMajorVersion = scalaVersion.substring(0, scalaVersion.lastIndexOf('.'));
            this.capability = capability;
        }

        public String getName() {
            return name.toString();
        }

        public String getVariantName(String prefix) {
            return prefix + StringGroovyMethods.capitalize(name);
        }

        public boolean isDefaultVariant() {
            return isDefaultVariant;
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

        public String getCapability() {
            return capability;
        }

        public String getSourceSetName(String sourceSetName) {
            String result;
            if (isDefaultVariant) {
                result = sourceSetName;
            } else {
                if (MAIN_SOURCE_SET_NAME.equals(sourceSetName)) {
                    result = name.toString();
                } else {
                    result = sourceSetName + StringGroovyMethods.capitalize(name);
                }
            }
            return result;
        }

        public String configuration(CharSequence configurationName) {
            return configuration(MAIN_SOURCE_SET_NAME, configurationName);
        }

        public String configuration(String sourceSet, CharSequence configurationName) {
            String result;
            if (isDefaultVariant && MAIN_SOURCE_SET_NAME.equals(sourceSet)) {
                result = configurationName.toString();
            } else {
                result = getSourceSetName(sourceSet) + StringGroovyMethods.capitalize(configurationName);
            }
            return result;
        }

        public String taskName(CharSequence taskName) {
            return isDefaultVariant ? taskName.toString() : name + StringGroovyMethods.capitalize(taskName);
        }

        public String testTaskName() {
            return isDefaultVariant ? TEST_TASK_NAME : TEST_TASK_NAME + StringGroovyMethods.capitalize(name);
        }

        public String itestTaskName() {
            return isDefaultVariant ? "integrationTest" : "integrationTest" + StringGroovyMethods.capitalize(name);
        }

        public String getArtifactConfiguration() {
            return name.toString();
        }
    }

    public static class SparkVariantPluginExtension {

        private final NamedDomainObjectSet<SparkVariant> variants;
        private String capability = null;
        private SparkVariant defaultVariant = null;

        public SparkVariantPluginExtension(Project project) {
            this.variants = project.container(SparkVariant.class);
        }

        public void capabilityGroup(String capability) {
            this.capability = capability;
        }

        public SparkVariant setDefaultVariant(String variantName, String sparkVersion, String scalaVersion) {
            if (defaultVariant != null) {
                throw new GradleException("Cannot set default variant multiple times");
            }
            if (capability == null) {
                throw new GradleException("Must set capability group before adding variant definitions");
            }
            defaultVariant = new SparkVariant(variantName, true, sparkVersion, scalaVersion, capability);
            variants.add(defaultVariant);
            return defaultVariant;
        }

        public SparkVariant addFeatureVariant(String variantName, String sparkVersion, String scalaVersion) {
            if (capability == null) {
                throw new GradleException("Must set capability group before adding variant definitions");
            }
            SparkVariant variant = new SparkVariant(variantName, false, sparkVersion, scalaVersion, capability);
            variants.add(variant);
            return variant;
        }

        public void all(Closure configure) {
            all(ConfigureUtil.configureUsing(configure));
        }

        public void all(Action<SparkVariant> action) {
            variants.all(action);
        }

        public void defaultVariant(Closure configure) {
            defaultVariant(ConfigureUtil.configureUsing(configure));
        }

        public void defaultVariant(Action<SparkVariant> action) {
            variants.matching(SparkVariant::isDefaultVariant).all(action);
        }

        public void featureVariants(Closure configure) {
            featureVariants(ConfigureUtil.configureUsing(configure));
        }

        public void featureVariants(Action<SparkVariant> action) {
            variants.matching(element -> !element.isDefaultVariant()).all(action);
        }

        public SparkVariant featureVariant(String featureVariant, Closure configure) {
            return featureVariant(featureVariant, ConfigureUtil.configureUsing(configure));
        }

        public SparkVariant featureVariant(String featureVariant, Action<SparkVariant> action) {
            return variants.getByName(featureVariant, action);
        }
    }

    /**
     * A rule that takes in a dependency component, checks if it is a distribution of the scala-library, and annotates it with a capability.
     */
    public static class ScalaRuntimeCapability implements ComponentMetadataRule {
        private final static String SCALA_LIBRARY = "scala-library";

        @Override
        public void execute(ComponentMetadataContext componentMetadataContext) {
            if (componentMetadataContext.getDetails() instanceof ComponentMetadataDetailsAdapter) {
                final ComponentMetadataDetailsAdapter details = (ComponentMetadataDetailsAdapter) componentMetadataContext.getDetails();
                if (SCALA_LIBRARY.equals(details.getId().getName())) {
                    details.allVariants(variantMetadata -> {
                        variantMetadata.withCapabilities(capabilityMetadata -> {
                            capabilityMetadata.addCapability("org.elasticsearch.gradle", SCALA_LIBRARY, details.getId().getVersion());
                        });
                    });
                }
            }
        }
    }

    // TODO: address deprecated configuration names
    private static List<String> TEST_CONFIGURATIONS_EXTENDED = Arrays.asList(
            COMPILE_CONFIGURATION_NAME,
            IMPLEMENTATION_CONFIGURATION_NAME,
            RUNTIME_CONFIGURATION_NAME,
            RUNTIME_ONLY_CONFIGURATION_NAME
    );

    @Override
    public void apply(final Project project) {
        SparkVariantPluginExtension extension = project.getExtensions().create("sparkVariants", SparkVariantPluginExtension.class, project);
        final JavaPluginConvention javaPluginConvention = project.getConvention().getPlugin(JavaPluginConvention.class);
        final JavaPluginExtension javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);

        // Add a rule that annotates scala-library dependencies with the scala-library capability
        project.getDependencies().getComponents().all(ScalaRuntimeCapability.class);

        extension.defaultVariant(sparkVariant -> configureDefaultVariant(project, sparkVariant, javaPluginExtension, javaPluginConvention));
        extension.featureVariants(sparkVariant -> configureVariant(project, sparkVariant, javaPluginExtension, javaPluginConvention));
        extension.all(sparkVariant -> registerArtifacts(project, sparkVariant));
    }

    private static void configureDefaultVariant(Project project, SparkVariant sparkVariant, JavaPluginExtension javaPluginExtension,
                                                JavaPluginConvention javaPluginConvention) {
        ConfigurationContainer configurations = project.getConfigurations();
        String capability = sparkVariant.capability + ":" + sparkVariant.getName() + ":" + project.getVersion().toString();

        Configuration apiElements = configurations.getByName(API_ELEMENTS_CONFIGURATION_NAME);
        apiElements.getOutgoing().capability(capability);

        Configuration runtimeElements = configurations.getByName(RUNTIME_ELEMENTS_CONFIGURATION_NAME);
        runtimeElements.getOutgoing().capability(capability);

        SourceSetContainer sourceSets = javaPluginConvention.getSourceSets();
        SourceSet testSourceSet = sourceSets.getByName(sparkVariant.getSourceSetName(TEST_SOURCE_SET_NAME));
        javaPluginExtension.registerFeature(sparkVariant.getVariantName("test"), featureSpec -> {
            featureSpec.usingSourceSet(testSourceSet);
            featureSpec.capability(sparkVariant.capability, sparkVariant.getVariantName("test"), project.getVersion().toString());
        });
    }

    private static void configureVariant(Project project, SparkVariant sparkVariant, JavaPluginExtension javaPluginExtension,
                                         JavaPluginConvention javaPluginConvention) {
        SourceSetContainer sourceSets = javaPluginConvention.getSourceSets();
        ConfigurationContainer configurations = project.getConfigurations();
        TaskContainer tasks = project.getTasks();
        String version = project.getVersion().toString();

        // Create a main and test source set for this variant
        SourceSet main = createVariantSourceSet(project, sparkVariant, sourceSets, MAIN_SOURCE_SET_NAME);
        SourceSet test = createVariantSourceSet(project, sparkVariant, sourceSets, TEST_SOURCE_SET_NAME);

        // Each variant's test source set is registered like just another variant in Gradle. These variants do not get any of the special
        // treatment needed in order to function like the testing part of a regular project. We need to do some basic wiring in the test
        // source set ourselves in order to get there.
        String testCompileClasspathName = sparkVariant.configuration(TEST_SOURCE_SET_NAME, COMPILE_CLASSPATH_CONFIGURATION_NAME);
        Configuration testCompileClasspath = configurations.getByName(testCompileClasspathName);
        test.setCompileClasspath((project.files(main.getOutput(), testCompileClasspath)));

        String testRuntimeClasspathName = sparkVariant.configuration(TEST_SOURCE_SET_NAME, RUNTIME_CLASSPATH_CONFIGURATION_NAME);
        Configuration testRuntimeClasspath = configurations.getByName(testRuntimeClasspathName);
        test.setRuntimeClasspath(project.files(test.getOutput(), main.getOutput(), testRuntimeClasspath));

        // Register our source sets as variants in the project
        registerVariants(javaPluginExtension, sparkVariant, main, test, version);
        extendParentConfigurations(configurations, sparkVariant);

        // Task Creation and Configuration
        createVariantTestTask(tasks, sparkVariant, test);
        configureVariantJar(tasks, sparkVariant);
        registerVariantScaladoc(project, tasks, sparkVariant, main);
    }

    private static SourceSet createVariantSourceSet(Project project, SparkVariant sparkVariant, SourceSetContainer sourceSets, String srcSetName) {
        SourceSet sourceSet = sourceSets.create(sparkVariant.getSourceSetName(srcSetName));

        SourceDirectorySet javaSourceSet = sourceSet.getJava();
        javaSourceSet.setSrcDirs(Collections.singletonList("src/" + srcSetName + "/java"));

        SourceDirectorySet scalaSourceSet = getScalaSourceSet(sourceSet).getScala();
        scalaSourceSet.setSrcDirs(Arrays.asList(
                "src/" + srcSetName + "/scala",
                "src/" + srcSetName + "/" + sparkVariant.getName()
        ));

        return sourceSet;
    }

    private static DefaultScalaSourceSet getScalaSourceSet(SourceSet sourceSet) {
        Convention sourceSetConvention = (Convention) InvokerHelper.getProperty(sourceSet, "convention");
        return (DefaultScalaSourceSet) sourceSetConvention.getPlugins().get("scala");
    }

    private static void registerVariants(JavaPluginExtension java, SparkVariant sparkVariant, SourceSet main, SourceSet test,
                                         String version) {
        // Register variant main source
        java.registerFeature(sparkVariant.getName(), featureSpec -> {
            featureSpec.usingSourceSet(main);
            featureSpec.capability(sparkVariant.getCapability(), sparkVariant.getName(), version);
            featureSpec.withJavadocJar();
            featureSpec.withSourcesJar();
        });

        // Register variant test source
        java.registerFeature(sparkVariant.getVariantName("test"), featureSpec -> {
            featureSpec.usingSourceSet(test);
            featureSpec.capability(sparkVariant.getCapability(), sparkVariant.getVariantName("test"), version);
        });
    }

    private static void extendParentConfigurations(ConfigurationContainer configurations, SparkVariant sparkVariant) {
        for (String configurationName : TEST_CONFIGURATIONS_EXTENDED) {
            Configuration mainConfiguration = configurations.getByName(sparkVariant.configuration(MAIN_SOURCE_SET_NAME, configurationName));
            Configuration testConfiguration = configurations.getByName(sparkVariant.configuration(TEST_SOURCE_SET_NAME, configurationName));
            testConfiguration.extendsFrom(mainConfiguration);
        }
    }

    private static void createVariantTestTask(TaskContainer tasks, SparkVariant sparkVariant, SourceSet test) {
        Test variantTestTask = tasks.create(sparkVariant.testTaskName(), Test.class);
        variantTestTask.setGroup(VERIFICATION_GROUP);
        variantTestTask.setTestClassesDirs(test.getOutput().getClassesDirs());
        variantTestTask.setClasspath(test.getRuntimeClasspath());

        Task check = tasks.getByName(JavaBasePlugin.CHECK_TASK_NAME);
        check.dependsOn(variantTestTask);
    }

    private static void configureVariantJar(TaskContainer tasks, SparkVariant sparkVariant) {
        Task build = tasks.getByName(BasePlugin.ASSEMBLE_TASK_NAME);
        build.dependsOn(sparkVariant.taskName(JavaPlugin.JAR_TASK_NAME));
    }

    private static void registerVariantScaladoc(Project project, TaskContainer tasks, SparkVariant sparkVariant, SourceSet main) {
        TaskProvider<ScalaDoc> scalaDocProvider = tasks.register(sparkVariant.taskName(SCALA_DOC_TASK_NAME), ScalaDoc.class);
        scalaDocProvider.configure(scalaDoc -> {
            scalaDoc.setGroup(DOCUMENTATION_GROUP);
            scalaDoc.setDescription("Generates Scaladoc for the " + sparkVariant.getSourceSetName(MAIN_SOURCE_SET_NAME) + " source code.");

            ConfigurableFileCollection scaladocClasspath = project.files();
            scaladocClasspath.from(main.getOutput());
            scaladocClasspath.from(main.getCompileClasspath());

            scalaDoc.setClasspath(scaladocClasspath);
            scalaDoc.setSource(getScalaSourceSet(main).getScala());
        });
    }

    private static void registerArtifacts(Project project, SparkVariant sparkVariant) {
        Configuration variantArtifact = project.getConfigurations().create(sparkVariant.getArtifactConfiguration());
        variantArtifact.setCanBeConsumed(true);
        variantArtifact.setCanBeResolved(false);
        Jar variantJar = (Jar) project.getTasks().getByName(sparkVariant.taskName(JAR_TASK_NAME));
        project.getArtifacts().add(sparkVariant.getArtifactConfiguration(), variantJar);
    }
}
