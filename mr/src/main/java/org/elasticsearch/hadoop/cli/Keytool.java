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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.hadoop.security.EsHadoopSecurityException;
import org.elasticsearch.hadoop.security.KeystoreWrapper;

public class Keytool {

    private static final String KEYSTORE_FILE_NAME = "esh.keystore";

    public static void main(String[] args) {
        System.exit(execute(new ConsolePrompt(), args));
    }

    public static int execute(Prompt console, String[] args) {
        Command command = null;
        String commandArg = null;
        boolean hasSTDIN = false;
        boolean force = false;
        for (int idx = 0; idx < args.length; idx++) {
            String arg = args[idx];
            if (arg.equals(FLAG_H) || arg.equals(FLAG_HELP)) {
                printUsage(console);
                return 0;
            } else if (hasSTDIN == false && (arg.equals(FLAG_STDIN))) {
                hasSTDIN = true;
            } else if (force == false && (arg.equals(FLAG_F) || arg.equals(FLAG_FORCE))) {
                force = true;
            } else if (command == null) {
                command = Command.byName(arg.toLowerCase());
                if (command == null) {
                    printUsage(console);
                    error(console, "Unknown command [" + arg + "]");
                    return 2;
                }
            } else if (command.hasArgs() && commandArg == null) {
                commandArg = arg;
            } else {
                printUsage(console);
                error(console, "Unexpected argument [" + arg + "]");
                return 3;
            }
        }
        if (command == null) {
            printUsage(console);
            error(console, "No command specified");
            return 1;
        }
        if (command.hasArgs && commandArg == null) {
            printUsage(console);
            error(console, "Settings name required for command [" + command.text + "]");
            return 4;
        }
        // If stdin is from a pipe, then the console will be null. Treat this like --stdin.
        hasSTDIN |= (System.console() == null);
        try {
            return new Keytool(console, command).run(commandArg, hasSTDIN, force);
        } catch (IOException e) {
            console.println("ERROR: " + e.getMessage());
            return 11;
        }
    }

    private static final String TBLFRMT = "%-15s%-19s%n";

    private static void printUsage(Prompt prompt) {
        prompt.println("A tool for managing settings stored in an ES-Hadoop keystore");
        prompt.println();
        prompt.println("Commands");
        prompt.println("--------");
        prompt.println(Command.CREATE.getText() + " - Creates a new elasticsearch keystore");
        prompt.println(Command.LIST.getText() + " - List entries in the keystore");
        prompt.println(Command.ADD.getText() + " - Add a setting to the keystore");
        prompt.println(Command.REMOVE.getText() + " - Remove a setting from the keystore");
        prompt.println();
        prompt.printf(TBLFRMT, "Option", "Description");
        prompt.printf(TBLFRMT, "------", "-----------");
        prompt.printf(TBLFRMT, FLAG_H + ", " + FLAG_HELP, "show help");
        prompt.printf(TBLFRMT, FLAG_F + ", " + FLAG_FORCE, "ignore overwriting warnings when adding to the keystore");
    }

    private static void error(Prompt prompt, String error) {
        prompt.println("ERROR: " + error);
    }

    private static final String FLAG_HELP = "--help";
    private static final String FLAG_H = "-h";

    private static final String FLAG_FORCE = "--force";
    private static final String FLAG_F = "-f";

    private static final String FLAG_STDIN = "--stdin";

    enum Command {
        CREATE("create", false),
        LIST("list", false),
        ADD("add", true),
        REMOVE("remove", true);

        private final String text;
        private final boolean hasArgs;

        Command(String text, boolean hasArgs) {
            this.text = text;
            this.hasArgs = hasArgs;
        }

        public String getText() {
            return text;
        }

        public boolean hasArgs() {
            return hasArgs;
        }

        private static Map<String, Command> lookup = new HashMap<String, Command>();

        static {
            lookup.put(CREATE.text, CREATE);
            lookup.put(LIST.text, LIST);
            lookup.put(ADD.text, ADD);
            lookup.put(REMOVE.text, REMOVE);
        }

        public static Command byName(String name) {
            return lookup.get(name);
        }
    }

    private final Prompt prompt;
    private final Command command;

    Keytool(Prompt prompt, Command command) {
        this.prompt = prompt;
        this.command = command;
    }

    /**
     * Run the program with the given arguments
     *
     * @param arg   Usually the name of the property to set. Can be empty for some commands
     * @param stdin Flag to state that the input should be read from stdin
     * @param force Flag to state that overwriting a key should be ignored
     * @return exit code
     */
    public int run(String arg, boolean stdin, boolean force) throws IOException {
        OutputStream outputStream = null;
        InputStream inputStream = null;
        KeystoreWrapper keystoreWrapper;
        try {
            switch (command) {
                case CREATE:
                    if (ksExists()) {
                        boolean proceed = promptYesNo("An es-hadoop keystore already exists. Overwrite? [y/N]");
                        if (proceed == false) {
                            prompt.println("Exiting without creating keystore");
                            return 0;
                        }
                    }
                    keystoreWrapper = KeystoreWrapper.newStore().build();
                    outputStream = openWrite();
                    keystoreWrapper.saveKeystore(outputStream);
                    return 0;
                case LIST:
                    if (!ksExists()) {
                        prompt.printf("ERROR: ES-Hadoop keystore not found. Use '%s' command to create one.%n", Command.CREATE.getText());
                        return 5;
                    }
                    inputStream = openRead();
                    keystoreWrapper = KeystoreWrapper.loadStore(inputStream).build();
                    for (String entry : keystoreWrapper.listEntries()) {
                        prompt.println(entry);
                    }
                    return 0;
                case ADD:
                    if (!ksExists()) {
                        prompt.printf("ERROR: ES-Hadoop keystore not found. Use '%s' command to create one.%n", Command.CREATE.getText());
                        return 5;
                    }
                    inputStream = openRead();
                    keystoreWrapper = KeystoreWrapper.loadStore(inputStream).build();
                    if (keystoreWrapper.containsEntry(arg) && force == false) {
                        boolean proceed = promptYesNo("Setting %s already exists. Overwrite? [y/N]", arg);
                        if (proceed == false) {
                            prompt.println("Exiting without modifying keystore");
                            return 0;
                        }
                    }
                    if (stdin) {
                        String data = prompt.readLine();
                        keystoreWrapper.setSecureSetting(arg, data);
                    } else {
                        char[] data = prompt.readPassword("Enter value for %s:", arg);
                        keystoreWrapper.setSecureSetting(arg, new String(data));
                        Arrays.fill(data, (char) 0);
                    }
                    outputStream = openWrite();
                    keystoreWrapper.saveKeystore(outputStream);
                    return 0;
                case REMOVE:
                    if (!ksExists()) {
                        prompt.printf("ERROR: ES-Hadoop keystore not found. Use '%s' command to create one.%n", Command.CREATE.getText());
                        return 5;
                    }
                    inputStream = openRead();
                    keystoreWrapper = KeystoreWrapper.loadStore(inputStream).build();
                    if (keystoreWrapper.containsEntry(arg) == false) {
                        prompt.printf("ERROR: Setting [%s] does not exist in the keystore.%n", arg);
                        return 6;
                    }
                    keystoreWrapper.removeSecureSetting(arg);
                    outputStream = openWrite();
                    keystoreWrapper.saveKeystore(outputStream);
                    return 0;
                default:
                    prompt.println("ERROR: Unsupported command " + command.getText());
                    return 7;
            }
        } catch (EsHadoopSecurityException ehse) {
            prompt.println("ERRORCould not load keystore file: " + ehse.getMessage());
            return 8;
        } catch (FileNotFoundException fnfe) {
            prompt.println("ERROR: Could not load keystore file: " + fnfe.getMessage());
            return 9;
        } catch (IOException ioe) {
            prompt.println("ERROR: " + ioe.getMessage());
            return 10;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    private boolean promptYesNo(String msgFormat, Object... args) {
        while (true) {
            String response = prompt.readLine(msgFormat, args);
            if (response == null || response.isEmpty() || response.equalsIgnoreCase("n")) {
                return false;
            } else if (response.equalsIgnoreCase("y")) {
                return true;
            } else {
                prompt.printf("Did not understand answer '%s'%n", response);
            }
        }
    }

    protected boolean ksExists() {
        File file = new File(KEYSTORE_FILE_NAME);
        return file.exists();
    }

    protected InputStream openRead() throws FileNotFoundException {
        File file = new File(KEYSTORE_FILE_NAME);
        return new FileInputStream(file);
    }

    protected OutputStream openWrite() throws IOException {
        File file = new File(KEYSTORE_FILE_NAME);
        if (file.exists()) {
            // Ignore return value
            file.createNewFile();
        }
        return new FileOutputStream(file);
    }
}
