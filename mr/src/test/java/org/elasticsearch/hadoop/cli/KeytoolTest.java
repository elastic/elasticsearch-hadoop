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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.elasticsearch.hadoop.security.KeystoreWrapper;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class KeytoolTest {

    @Test
    public void executeNoCommand() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{}), equalTo(1));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeNoCommandWithArgs() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"--stdin"}), equalTo(1));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeHelp() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"-h"}), equalTo(0));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeHelpExt() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"--help"}), equalTo(0));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeCommandFail() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"blah"}), equalTo(2));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeCommandFailWithHelp() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"blah", "--help"}), equalTo(2));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeAddFailNoArgument() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"add"}), equalTo(4));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeAddFailNoSettingsName() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"add", "--stdin"}), equalTo(4));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeAddFailUnknownArgument() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"add", "--stdin", "property.name", "someOtherTHing"}), equalTo(3));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeAdd() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"add", "--stdin", "property.name", "someOtherTHing"}), equalTo(3));
        assertHelpMessage(console.getOutput());
    }

    @Test
    public void executeListFailUnknownArgument() {
        TestPrompt console = new TestPrompt();
        assertThat(Keytool.execute(console, new String[]{"list", "property.name"}), equalTo(3));
        assertHelpMessage(console.getOutput());
    }

    private static final String HELP = "A tool for managing settings stored in an ES-Hadoop keystore\n" +
            "\n" +
            "Commands\n" +
            "--------\n" +
            "create - Creates a new elasticsearch keystore\n" +
            "list - List entries in the keystore\n" +
            "add - Add a setting to the keystore\n" +
            "remove - Remove a setting from the keystore\n" +
            "\n" +
            "Option         Description        \n" +
            "------         -----------        \n" +
            "-h, --help     show help          \n" +
            "-f, --force    ignore overwriting warnings when adding to the keystore";

    private static void assertHelpMessage(String output) {
        assertThat(output, containsString(HELP));
    }

    private static class KeytoolHarness extends Keytool {
        private boolean exists;
        private BytesArray fileBytes;

        KeytoolHarness(Prompt prompt, Keytool.Command command, boolean exists, BytesArray fileBytes) {
            super(prompt, command);
            this.exists = exists;
            this.fileBytes = fileBytes;
        }

        @Override
        protected InputStream openRead() throws FileNotFoundException {
            return new FastByteArrayInputStream(fileBytes);
        }

        @Override
        protected OutputStream openWrite() throws IOException {
            this.exists = true;
            this.fileBytes.reset();
            return new FastByteArrayOutputStream(fileBytes);
        }

        @Override
        protected boolean ksExists() {
            return exists;
        }

        public BytesArray getFileBytes() {
            return fileBytes;
        }
    }

    @Test
    public void createKeystore() throws Exception {
        TestPrompt console = new TestPrompt();
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.CREATE, false, new BytesArray(128));
        assertThat(keytool.run(null, false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
    }

    @Test
    public void createKeystoreExistsAlreadyOverwrite() throws Exception {
        TestPrompt console = new TestPrompt();
        console.addInput("y");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.CREATE, true, new BytesArray(128));
        assertThat(keytool.run(null, false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
    }

    @Test
    public void createKeystoreExistsAlreadyCancel() throws Exception {
        TestPrompt console = new TestPrompt();
        console.addInput("n");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.CREATE, true, new BytesArray(128));
        assertThat(keytool.run(null, false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo("Exiting without creating keystore\n"));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(0));
    }

    @Test
    public void createKeystoreExistsAlreadyCancelAfterGarbage() throws Exception {
        TestPrompt console = new TestPrompt();
        console.addInput("nope")
                .addInput("yup")
                .addInput("blahblahblah")
                .addInput("n");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.CREATE, true, new BytesArray(128));
        assertThat(keytool.run(null, false, false), equalTo(0));
        assertThat(
                console.getOutput(),
                equalTo("Did not understand answer 'nope'\n" +
                        "Did not understand answer 'yup'\n" +
                        "Did not understand answer 'blahblahblah'\n" +
                        "Exiting without creating keystore\n"
                )
        );
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(0));
    }

    @Test
    public void listKeystoreNonExistant() throws Exception {
        TestPrompt console = new TestPrompt();
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.LIST, false, new BytesArray(128));
        assertThat(keytool.run(null, false, false), equalTo(5));
        assertThat(console.getOutput(), equalTo("ERROR: ES-Hadoop keystore not found. Use 'create' command to create one.\n"));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(false));
        assertThat(keytool.fileBytes.length(), is(0));
    }

    @Test
    public void listKeystoreEmpty() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper.newStore().build().saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.LIST, true, storeData);
        assertThat(keytool.run(null, false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
    }

    @Test
    public void listKeystore() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.setSecureSetting("test.password.1", "blah");
        ks.setSecureSetting("test.password.2", "blah");
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.LIST, true, storeData);
        assertThat(keytool.run(null, false, false), equalTo(0));
        assertThat(
                console.getOutput(),
                equalTo(
                        "test.password.1\n" +
                        "test.password.2\n"
                )
        );
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
    }

    @Test
    public void addNonExistant() throws Exception {
        TestPrompt console = new TestPrompt();
        console.addInput("blah");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.ADD, false, new BytesArray(128));
        assertThat(keytool.run("test.password.1", false, false), equalTo(5));
        assertThat(console.getOutput(), equalTo("ERROR: ES-Hadoop keystore not found. Use 'create' command to create one.\n"));
        assertThat(console.hasInputLeft(), is(true));
        assertThat(keytool.ksExists(), is(false));
        assertThat(keytool.fileBytes.length(), is(0));
    }

    @Test
    public void addExistingKeyCancel() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.setSecureSetting("test.password.1", "blah");
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        console.addInput("n");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.ADD, true, storeData);
        assertThat(keytool.run("test.password.1", false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo("Exiting without modifying keystore\n"));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
    }

    @Test
    public void addExistingKeyOverwrite() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.setSecureSetting("test.password.1", "blah");
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        console.addInput("y").addInput("blerb");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.ADD, true, storeData);
        assertThat(keytool.run("test.password.1", false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
        ks = KeystoreWrapper.loadStore(new FastByteArrayInputStream(keytool.fileBytes)).build();
        assertThat(ks.getSecureSetting("test.password.1"), equalTo("blerb"));
    }

    @Test
    public void addExistingKeyForce() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.setSecureSetting("test.password.1", "blah");
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        console.addInput("blerb");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.ADD, true, storeData);
        assertThat(keytool.run("test.password.1", false, true), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
        ks = KeystoreWrapper.loadStore(new FastByteArrayInputStream(keytool.fileBytes)).build();
        assertThat(ks.getSecureSetting("test.password.1"), equalTo("blerb"));
    }

    @Test
    public void addKey() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        console.addInput("blahh");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.ADD, true, storeData);
        assertThat(keytool.run("test.password.1", false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
        ks = KeystoreWrapper.loadStore(new FastByteArrayInputStream(keytool.fileBytes)).build();
        assertThat(ks.getSecureSetting("test.password.1"), equalTo("blahh"));
    }

    @Test
    public void addKeyStdIn() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        console.addInput("blahh");
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.ADD, true, storeData);
        assertThat(keytool.run("test.password.1", true, false), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
        ks = KeystoreWrapper.loadStore(new FastByteArrayInputStream(keytool.fileBytes)).build();
        assertThat(ks.getSecureSetting("test.password.1"), equalTo("blahh"));
    }

    @Test
    public void removeNonExistant() throws Exception {
        TestPrompt console = new TestPrompt();
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.REMOVE, false, new BytesArray(128));
        assertThat(keytool.run("test.password.1", false, false), equalTo(5));
        assertThat(console.getOutput(), equalTo("ERROR: ES-Hadoop keystore not found. Use 'create' command to create one.\n"));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(false));
        assertThat(keytool.fileBytes.length(), is(0));
    }

    @Test
    public void removeMissingKey() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.REMOVE, true, storeData);
        assertThat(keytool.run("test.password.1", false, false), equalTo(6));
        assertThat(console.getOutput(), equalTo("ERROR: Setting [test.password.1] does not exist in the keystore.\n"));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
    }

    @Test
    public void removeKey() throws Exception {
        BytesArray storeData = new BytesArray(128);
        KeystoreWrapper ks = KeystoreWrapper.newStore().build();
        ks.setSecureSetting("test.password.1", "bar");
        ks.setSecureSetting("test.password.2", "foo");
        ks.saveKeystore(new FastByteArrayOutputStream(storeData));
        TestPrompt console = new TestPrompt();
        KeytoolHarness keytool = new KeytoolHarness(console, Keytool.Command.REMOVE, true, storeData);
        assertThat(keytool.run("test.password.1", false, false), equalTo(0));
        assertThat(console.getOutput(), equalTo(""));
        assertThat(console.hasInputLeft(), is(false));
        assertThat(keytool.ksExists(), is(true));
        assertThat(keytool.fileBytes.length(), is(not(0)));
        ks = KeystoreWrapper.loadStore(new FastByteArrayInputStream(keytool.fileBytes)).build();
        assertThat(ks.containsEntry("test.password.1"), is(false));
        assertThat(ks.getSecureSetting("test.password.2"), equalTo("foo"));
    }

}