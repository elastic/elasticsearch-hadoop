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

package org.elasticsearch.hadoop.gradle.tasks

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.TaskAction

class VerifyChecksums extends DefaultTask {

    private File inputFile
    private Map<String, String> checksums = new HashMap<>()

    @InputFile
    public File getInputFile() {
        return this.inputFile
    }

    public void inputFile(File file) {
        this.inputFile = file
    }

    @Input
    public Map<String, String> getChecksums() {
        return this.checksums
    }

    public void checksum(String algorithm, String result) {
        this.checksums.put(algorithm, result)
    }

    public void checksums(Map<String, String> additions) {
        this.checksums.putAll(additions)
    }

    @TaskAction
    public void verifyChecksums() {
        if (inputFile == null) {
            throw new GradleException("Input file required on verify checksums task")
        }
        AntBuilder antBuilder = project.getAnt()
        checksums.collect { String algorithmName, String expected ->
            String verifyPropertyName = "${getName()}.${algorithmName}.result"
            antBuilder.checksum(
                    file: inputFile.absolutePath,
                    algorithm: algorithmName,
                    property: "${verifyPropertyName}",
            )
            String expectedHash = expected.toUpperCase()
            String actualHash = antBuilder.properties[verifyPropertyName].toString().toUpperCase()
            boolean success = actualHash.equals(expectedHash)
            logger.info("Validation of [${algorithmName}] checksum was [${success ? "successful" : "failure"}]")
            if (!success) {
                throw new GradleException("Failed to verify [${inputFile}] against [${algorithmName}] checksum.\n" +
                        "Expected [${expectedHash}]\n" +
                        " but got [${actualHash}].")
            }
        }
    }
}
