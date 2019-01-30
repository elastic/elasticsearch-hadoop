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

package org.elasticsearch.hadoop.qa.kerberos.security;

import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

public final class KeytabLogin {

    private static final Log LOG = LogFactory.getLog(KeytabLogin.class);

    private static final String SYS_PRINCIPAL_NAME = "test.krb5.principal";
    private static final String SYS_KEYTAB_PATH = "test.krb5.keytab";

    public static <V> V doAfterLogin(PrivilegedExceptionAction<V> action) throws Exception {
        String principalName = System.getProperty(SYS_PRINCIPAL_NAME);
        String keytabPath = System.getProperty(SYS_KEYTAB_PATH);

        if (principalName == null && keytabPath == null) {
            LOG.warn("Principal name and keytab path are not provided. Skipping driver login.");
            return action.run();
        }

        if (principalName == null) {
            throw new IllegalArgumentException("Must specify principal name with ["+SYS_PRINCIPAL_NAME+"] java property");
        } else if (keytabPath == null) {
            throw new IllegalArgumentException("Must specify keytab path with ["+SYS_KEYTAB_PATH+"] java property");
        }

        UserGroupInformation.loginUserFromKeytab(principalName, keytabPath);
        LOG.info("Login complete");
        return UserGroupInformation.getCurrentUser().doAs(action);
    }

    private KeytabLogin() {}
}
