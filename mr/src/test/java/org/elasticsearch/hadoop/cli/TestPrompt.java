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

package org.elasticsearch.hadoop.cli;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class TestPrompt implements Prompt {

    private StringBuilder builder = new StringBuilder();
    private Deque<String> inputLines = new LinkedList<String>();

    @Override
    public void println() {
        builder.append("\n");
    }

    @Override
    public void println(String s) {
        builder.append(s).append("\n");
    }

    @Override
    public void printf(String format, Object... args) {
        builder.append(String.format(format, args));
    }

    @Override
    public String readLine() {
        String data = inputLines.pollFirst();
        if (data == null) {
            throw new AssertionError("Tried to read data beyond test specifications");
        }
        return data;
    }

    @Override
    public String readLine(String format, Object... args) {
        return readLine();
    }

    @Override
    public char[] readPassword(String prompt, Object... args) {
        return readLine().toCharArray();
    }

    TestPrompt addInput(String inputLine) {
        inputLines.add(inputLine);
        return this;
    }

    boolean hasInputLeft() {
        return !inputLines.isEmpty();
    }

    String getOutput() {
        return builder.toString();
    }
}
