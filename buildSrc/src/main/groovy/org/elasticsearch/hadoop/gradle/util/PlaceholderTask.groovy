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

package org.elasticsearch.hadoop.gradle.util

import org.gradle.api.Action
import org.gradle.api.AntBuilder
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.logging.Logger
import org.gradle.api.logging.LoggingManager
import org.gradle.api.plugins.Convention
import org.gradle.api.plugins.ExtensionContainer
import org.gradle.api.specs.Spec
import org.gradle.api.tasks.TaskDependency
import org.gradle.api.tasks.TaskDestroyables
import org.gradle.api.tasks.TaskInputs
import org.gradle.api.tasks.TaskLocalState
import org.gradle.api.tasks.TaskOutputs
import org.gradle.api.tasks.TaskState

/**
 * A placeholder task only useful for accumulating the list of dependencies and finalize tasks for a
 * task that is passed around to third party code.
 */
class PlaceholderTask implements Task {

    Set<Object> taskDeps = []
    Set<Object> taskFinalizers = []

    @Override
    String getName() {
        return "placeholder"
    }

    @Override
    Set<Object> getDependsOn() {
        return taskDeps
    }

    @Override
    void setDependsOn(Iterable<?> dependsOnTasks) {
        taskDeps = []
        taskDeps.addAll(dependsOnTasks)
    }

    @Override
    Task dependsOn(Object... paths) {
        taskDeps.addAll(paths)
        return this
    }

    @Override
    Task finalizedBy(Object... paths) {
        taskFinalizers.addAll(paths)
        return this
    }

    @Override
    void setFinalizedBy(Iterable<?> finalizedBy) {
        taskFinalizers = []
        taskFinalizers.addAll(finalizedBy)
    }

    @Override
    Project getProject() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    List<Action<? super Task>> getActions() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setActions(List<Action<? super Task>> actions) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskDependency getTaskDependencies() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void onlyIf(Closure onlyIfClosure) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void onlyIf(Spec<? super Task> onlyIfSpec) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setOnlyIf(Closure onlyIfClosure) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setOnlyIf(Spec<? super Task> onlyIfSpec) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskState getState() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setDidWork(boolean didWork) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    boolean getDidWork() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    String getPath() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task doFirst(Action<? super Task> action) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task doFirst(Closure action) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task doFirst(String actionName, Action<? super Task> action) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task doLast(Action<? super Task> action) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task doLast(String actionName, Action<? super Task> action) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task doLast(Closure action) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task leftShift(Closure action) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task deleteAllActions() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    boolean getEnabled() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task configure(Closure configureClosure) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    AntBuilder getAnt() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Logger getLogger() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    LoggingManager getLogging() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Object property(String propertyName) throws MissingPropertyException {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    boolean hasProperty(String propertyName) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Convention getConvention() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    String getDescription() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setDescription(String description) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    String getGroup() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setGroup(String group) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    boolean dependsOnTaskDidWork() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskInputs getInputs() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskOutputs getOutputs() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskDestroyables getDestroyables() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskLocalState getLocalState() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    File getTemporaryDir() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    Task mustRunAfter(Object... paths) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setMustRunAfter(Iterable<?> mustRunAfter) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskDependency getMustRunAfter() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskDependency getFinalizedBy() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskDependency shouldRunAfter(Object... paths) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    void setShouldRunAfter(Iterable<?> shouldRunAfter) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    TaskDependency getShouldRunAfter() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    int compareTo(Task o) {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }

    @Override
    ExtensionContainer getExtensions() {
        throw new UnsupportedOperationException("Placeholder task cannot perform operations")
    }
}
