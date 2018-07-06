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

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConsolePrompt implements Prompt {
    private final Console console;

    public ConsolePrompt() {
        this.console = System.console();
    }

    @Override
    public void println() {
        System.out.println();
    }

    @Override
    public void println(String s) {
        System.out.println(s);
    }

    @Override
    public void printf(String format, Object... args) {
        System.out.printf(format, args);
    }

    @Override
    public String readLine() {
        if (console == null) {
            try {
                return new BufferedReader(new InputStreamReader(System.in)).readLine();
            } catch (IOException e) {
                throw new IOError(e);
            }
        } else {
            return this.console.readLine();
        }
    }

    @Override
    public String readLine(String format, Object... args) {
        printf(format, args);
        return readLine();
    }

    @Override
    public char[] readPassword(String prompt, Object... args) {
        if (console == null) {
            throw new IllegalStateException("Cannot disable console echo to read password");
        }
        return this.console.readPassword(prompt, args);
    }
}
