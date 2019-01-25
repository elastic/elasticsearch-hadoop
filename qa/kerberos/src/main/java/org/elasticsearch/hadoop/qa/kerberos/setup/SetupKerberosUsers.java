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

import java.util.List;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;

public class SetupKerberosUsers {

    private static final String PRINCIPALS = "principals";

    public void run(String[] args) throws Exception {
        String rawPrincipals = getProperty(PRINCIPALS);

        Settings settings = new TestSettings();
        System.out.println(settings.getNodes());
        System.out.println(settings.getNetworkHttpAuthUser());
        System.out.println(settings.getNetworkHttpAuthPass());

        List<String> principals = StringUtils.tokenize(rawPrincipals);
        for (String principal : principals) {
            RestUtils.postData("_xpack/security/role_mapping/kerberos_client_mapping",
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
