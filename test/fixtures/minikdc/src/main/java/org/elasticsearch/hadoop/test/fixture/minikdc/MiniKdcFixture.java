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

package org.elasticsearch.hadoop.test.fixture.minikdc;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.minikdc.MiniKdc;

/**
 * Hadoop has a utility for making a Mini KDC for testing purposes. While this is great
 * and will limit our build dependencies for the time being, we need to wrap it in some
 * code that will export a ports and pid file, as well as do some provisioning of users
 * before start up completes.
 */
public class MiniKdcFixture {

    private static final String PORT_FILE_NAME = "ports";
    private static final String PID_FILE_NAME = "pid";

    private static final String SYS_ES_FIXTURE_KDC = "es.fixture.kdc.";

    private static final String SYS_USER_CONF = "user.";
    private static final String SYS_PWD_SUFFIX = ".pwd";
    private static final String SYS_KEYTAB_SUFFIX = ".keytab";

    private static final String SYS_PROP_CONF = "prop.";

    private static final int ARGS_EXPECTED = 1;
    private static final int ARG_BASE_DIR = 0;

    public static void main(String[] args) throws Exception {
        if (args.length != ARGS_EXPECTED) {
            throw new IllegalArgumentException("Expected: MiniKdcWrapper <baseDirectory>, got: " + Arrays.toString(args));
        }

        // Parse Work Dir
        File workDirFile = new File(args[ARG_BASE_DIR]);
        Path workDirPath = Paths.get(args[ARG_BASE_DIR]);

        // Provisioning info
        Map<String, String> userToPassword = new HashMap<>();
        Map<String, List<String>> keytabToUsers = new HashMap<>();
        Set<String> allKeytabUsers = new HashSet<>();

        // Create default KDC Conf
        Properties kdcConf = new Properties();
        // MiniKdc Defaults
        kdcConf.setProperty(MiniKdc.KDC_BIND_ADDRESS, "localhost");
        kdcConf.setProperty(MiniKdc.KDC_PORT, "0"); // ephemeral port
        kdcConf.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "86400000"); // 1 day
        kdcConf.setProperty(MiniKdc.MAX_RENEWABLE_LIFETIME, "604800000"); // 7 days
        kdcConf.setProperty(MiniKdc.DEBUG, "false"); // Sets krb library debug
        kdcConf.setProperty(MiniKdc.INSTANCE, "DefaultKrbServer");
        kdcConf.setProperty(MiniKdc.TRANSPORT, "TCP");
        // ES-Hadoop Project Specific Defaults
        kdcConf.setProperty(MiniKdc.ORG_NAME, "BUILD.ELASTIC"); // used to make REALM
        kdcConf.setProperty(MiniKdc.ORG_DOMAIN, "CO"); // used to make REALM

        // Parse Fixture Properties in one pass
        Enumeration<Object> sysProps = System.getProperties().keys();
        while (sysProps.hasMoreElements()) {
            String key = sysProps.nextElement().toString();
            if (key.startsWith(SYS_ES_FIXTURE_KDC)) {
                String subKey = key.substring(SYS_ES_FIXTURE_KDC.length());
                if (subKey.startsWith(SYS_USER_CONF)) {
                    String userConf = subKey.substring(SYS_USER_CONF.length());

                    // Check if password or keytab
                    if (userConf.endsWith(SYS_PWD_SUFFIX) && userConf.length() > SYS_PWD_SUFFIX.length()) {
                        String userName = userConf.substring(0, userConf.length() - SYS_PWD_SUFFIX.length());
                        if (userToPassword.containsKey(userName) || allKeytabUsers.contains(userName)) {
                            throw new IllegalArgumentException("Cannot add user twice!");
                        }
                        userToPassword.put(userName, System.getProperty(key));

                    } else if (userConf.endsWith(SYS_KEYTAB_SUFFIX) && userConf.length() > SYS_KEYTAB_SUFFIX.length()) {
                        String userName = userConf.substring(0, userConf.length() - SYS_KEYTAB_SUFFIX.length());
                        if (userToPassword.containsKey(userName) || allKeytabUsers.contains(userName)) {
                            throw new IllegalArgumentException("Cannot add user twice!");
                        }
                        String keytabFile = System.getProperty(key);
                        List<String> keytabUsers = keytabToUsers.get(keytabFile);
                        if (keytabUsers == null) {
                            keytabUsers = new ArrayList<>();
                            keytabToUsers.put(keytabFile, keytabUsers);
                        }
                        keytabUsers.add(userName);
                        allKeytabUsers.add(userName);

                    } else {
                        throw new IllegalArgumentException("Unknown user property : " + key);
                    }

                } else if (subKey.startsWith(SYS_PROP_CONF)) {
                    String propConf = subKey.substring(SYS_PROP_CONF.length());
                    kdcConf.put(propConf, System.getProperty(key));
                } else {
                    throw new IllegalArgumentException("Unknown property : " + key);
                }
            }
        }

        // Start up KDC
        final MiniKdc miniKdc = new MiniKdc(kdcConf, workDirFile);
        miniKdc.start();
        File krb5conf = new File(workDirFile, "krb5.conf");
        if (miniKdc.getKrb5conf().renameTo(krb5conf)) {

            System.out.println();
            System.out.println("Standalone MiniKdc Running");
            System.out.println("---------------------------------------------------");
            System.out.println("  Realm           : " + miniKdc.getRealm());
            System.out.println("  Running at      : " + miniKdc.getHost() + ":" + miniKdc.getPort());
            System.out.println("  krb5conf        : " + krb5conf);
            System.out.println();

            // Provision the principals
            for (Map.Entry<String, String> entry : userToPassword.entrySet()) {
                String user = entry.getKey();
                String password = entry.getValue();

                System.out.println("Creating principal [" + user + "]");
                miniKdc.createPrincipal(user, password);
            }

            for (Map.Entry<String, List<String>> entry : keytabToUsers.entrySet()) {
                String keytabName = entry.getKey();
                List<String> users = entry.getValue();

                System.out.println("Creating keytab [" + keytabName + "] for users " + users.toString());
                File keytabFile = new File(workDirFile, keytabName);
                miniKdc.createPrincipal(keytabFile, users.toArray(new String[0]));
            }

            System.out.println();
            System.out.println(" Do <CTRL-C> or kill <PID> to stop it");
            System.out.println("---------------------------------------------------");
            System.out.println();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    miniKdc.stop();
                }
            });
        } else {
            throw new RuntimeException("Cannot rename KDC's krb5conf to "
                    + krb5conf.getAbsolutePath());
        }

        // Write PID File
        Path tmp = Files.createTempFile(workDirPath, null, null);
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        Files.write(tmp, pid.getBytes(StandardCharsets.UTF_8));
        Files.move(tmp, workDirPath.resolve(PID_FILE_NAME), StandardCopyOption.ATOMIC_MOVE);

        // Write Ports File
        String portFileContent = Integer.toString(miniKdc.getPort());
        tmp = Files.createTempFile(workDirPath, null, null);
        Files.write(tmp, portFileContent.getBytes(StandardCharsets.UTF_8));
        Files.move(tmp, workDirPath.resolve(PORT_FILE_NAME), StandardCopyOption.ATOMIC_MOVE);
    }
}
