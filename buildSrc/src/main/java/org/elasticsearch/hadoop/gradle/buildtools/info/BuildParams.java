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
package org.elasticsearch.hadoop.gradle.buildtools.info;

import org.gradle.api.JavaVersion;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class BuildParams {
    private static File runtimeJavaHome;
    private static Boolean isRuntimeJavaHomeSet;
    private static List<JavaHome> javaVersions;
    private static JavaVersion minimumCompilerVersion;
    private static JavaVersion minimumRuntimeVersion;
    private static JavaVersion runtimeJavaVersion;
    private static Boolean inFipsJvm;
    private static String gitRevision;
    private static String testSeed;

    /**
     * Initialize global build parameters. This method accepts and a initialization function which in turn accepts a
     * {@link MutableBuildParams}. Initialization can be done in "stages", therefore changes override existing values, and values from
     * previous calls to {@link #init(Consumer)} carry forward. In cases where you want to clear existing values
     * {@link MutableBuildParams#reset()} may be used.
     *
     * @param initializer Build parameter initializer
     */
    public static void init(Consumer<MutableBuildParams> initializer) {
        initializer.accept(MutableBuildParams.INSTANCE);
    }

    public static File getRuntimeJavaHome() {
        return value(runtimeJavaHome);
    }

    public static Boolean getIsRuntimeJavaHomeSet() {
        return value(isRuntimeJavaHomeSet);
    }

    public static List<JavaHome> getJavaVersions() {
        return value(javaVersions);
    }

    public static JavaVersion getMinimumCompilerVersion() {
        return value(minimumCompilerVersion);
    }

    public static JavaVersion getMinimumRuntimeVersion() {
        return value(minimumRuntimeVersion);
    }

    public static JavaVersion getRuntimeJavaVersion() {
        return value(runtimeJavaVersion);
    }

    public static Boolean isInFipsJvm() {
        return value(inFipsJvm);
    }

    public static String getGitRevision() {
        return value(gitRevision);
    }

    public static String getTestSeed() {
        return value(testSeed);
    }

    private static <T> T value(T object) {
        if (object == null) {
            String callingMethod = Thread.currentThread().getStackTrace()[2].getMethodName();

            throw new IllegalStateException(
                "Build parameter '"
                    + propertyName(callingMethod)
                    + "' has not been initialized.\n"
                    + "Perhaps the plugin responsible for initializing this property has not been applied."
            );
        }

        return object;
    }

    private static String propertyName(String methodName) {
        String propertyName = methodName.startsWith("is") ? methodName.substring("is".length()) : methodName.substring("get".length());
        return propertyName.substring(0, 1).toLowerCase() + propertyName.substring(1);
    }

    public static class MutableBuildParams {
        private static MutableBuildParams INSTANCE = new MutableBuildParams();

        private MutableBuildParams() {}

        /**
         * Resets any existing values from previous initializations.
         */
        public void reset() {
            Arrays.stream(org.elasticsearch.hadoop.gradle.buildtools.info.BuildParams.class.getDeclaredFields()).filter(f -> Modifier.isStatic(f.getModifiers())).forEach(f -> {
                try {
                    // Since we are mutating private static fields from a public static inner class we need to suppress
                    // accessibility controls here.
                    f.setAccessible(true);
                    f.set(null, null);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public void setRuntimeJavaHome(File runtimeJavaHome) {
            BuildParams.runtimeJavaHome = requireNonNull(runtimeJavaHome);
        }

        public void setIsRuntimeJavaHomeSet(boolean isRutimeJavaHomeSet) {
            BuildParams.isRuntimeJavaHomeSet = isRutimeJavaHomeSet;
        }

        public void setJavaVersions(List<JavaHome> javaVersions) {
            BuildParams.javaVersions = requireNonNull(javaVersions);
        }

        public void setMinimumCompilerVersion(JavaVersion minimumCompilerVersion) {
            BuildParams.minimumCompilerVersion = requireNonNull(minimumCompilerVersion);
        }

        public void setMinimumRuntimeVersion(JavaVersion minimumRuntimeVersion) {
            BuildParams.minimumRuntimeVersion = requireNonNull(minimumRuntimeVersion);
        }

        public void setRuntimeJavaVersion(JavaVersion runtimeJavaVersion) {
            BuildParams.runtimeJavaVersion = requireNonNull(runtimeJavaVersion);
        }

        public void setInFipsJvm(boolean inFipsJvm) {
            BuildParams.inFipsJvm = inFipsJvm;
        }

        public void setGitRevision(String gitRevision) {
            BuildParams.gitRevision = requireNonNull(gitRevision);
        }

        public void setTestSeed(String testSeed) {
            BuildParams.testSeed = requireNonNull(testSeed);
        }
    }
}
