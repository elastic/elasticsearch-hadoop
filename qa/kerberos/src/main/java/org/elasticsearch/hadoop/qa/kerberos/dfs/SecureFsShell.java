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

package org.elasticsearch.hadoop.qa.kerberos.dfs;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.fs.FsShell;
import org.elasticsearch.hadoop.qa.kerberos.security.KeytabLogin;

/**
 * Using FsShell requires you to be logged into Kerberos from the command line (kinit).
 * Since it is not expected for the kerberos packages to be locally installed for builds
 * and testing, we wrap the FsShell program to login via keytab.
 */
public class SecureFsShell {

    public static void main(final String[] args) throws Exception {
        KeytabLogin.doAfterLogin(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                FsShell.main(args);
                return null;
            }
        });
    }

}
