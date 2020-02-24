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

package org.elasticsearch.hadoop.qa.kerberos.setup;

import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.qa.kerberos.ExtendedClient;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.StringUtils;

public class SetupKerberosUsers {

    private static final String USERS = "users";
    private static final String PRINCIPALS = "principals";
    private static final String PROXIERS = "proxiers";

    public void run(String[] args) throws Exception {
        String rawUsers = getProperty(USERS);
        String rawPrincipals = getProperty(PRINCIPALS);
        String rawProxiers = getProperty(PROXIERS);

        Settings settings = new PropertiesSettings();
        settings.asProperties().putAll(System.getProperties());
        System.out.println(settings.getNodes());
        System.out.println(settings.getNetworkHttpAuthUser());
        System.out.println(settings.getNetworkHttpAuthPass());

        InitializationUtils.discoverClusterInfo(settings, LogFactory.getLog(SetupKerberosUsers.class));

        int idx = 0;
        ExtendedClient client = new ExtendedClient(settings);

        for (String user : StringUtils.tokenize(rawUsers)) {
            client.post("_xpack/security/user/" + user, (
                    "{\n" +
                    "  \"enabled\" : true,\n" +
                    "  \"password\" : \"password\",\n" +
                    "  \"roles\" : [ \"superuser\" ],\n" +
                    "  \"full_name\" : \"Client "+user+"\"\n" +
                    "}").getBytes());
            System.out.println("Added user for [" + user + "]");
            idx++;
        }

        for (String principal : StringUtils.tokenize(rawPrincipals)) {
            client.post("_xpack/security/role_mapping/kerberos_client_mapping_"+idx,
                    ("{" +
                        "\"roles\":[\"superuser\"]," +
                        "\"enabled\":true," +
                        "\"rules\":{" +
                            "\"field\":{" +
                                "\"username\":\"" + principal + "\"" +
                            "}" +
                        "}" +
                    "}").getBytes()
            );
            System.out.println("Added role mapping for principal [" + principal + "]");
            idx++;
        }

        System.out.println("Creating proxy role");
        client.post("_xpack/security/role/proxier", (
                "{\n" +
                "  \"cluster\": [\n" +
                "    \"all\"\n" +
                "  ],\n" +
                "  \"indices\": [\n" +
                "    {\n" +
                "      \"names\": [\n" +
                "        \"*\"\n" +
                "      ],\n" +
                "      \"privileges\": [\n" +
                "        \"all\"\n" +
                "      ],\n" +
                "      \"allow_restricted_indices\": true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"applications\": [\n" +
                "    {\n" +
                "      \"application\": \"*\",\n" +
                "      \"privileges\": [\n" +
                "        \"*\"\n" +
                "      ],\n" +
                "      \"resources\": [\n" +
                "        \"*\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"run_as\": [\n" +
                "    \"*\"\n" +
                "  ],\n" +
                "  \"transient_metadata\": {}\n" +
                "}").getBytes());

        for (String proxier : StringUtils.tokenize(rawProxiers)) {
            client.post("_xpack/security/role_mapping/kerberos_client_mapping_"+idx,
                    ("{" +
                        "\"roles\":[\"proxier\"]," +
                        "\"enabled\":true," +
                        "\"rules\":{" +
                            "\"field\":{" +
                                "\"username\":\"" + proxier + "\"" +
                            "}" +
                        "}" +
                    "}").getBytes()
            );
            System.out.println("Added role mapping for principal [" + proxier + "] to perform impersonation");
            idx++;
        }
    }

    private String getProperty(String property) {
        String value = System.getProperty(property);
        if (value == null) {
            throw new IllegalArgumentException("Requires system property [" + property + "]");
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        new SetupKerberosUsers().run(args);
    }
}
