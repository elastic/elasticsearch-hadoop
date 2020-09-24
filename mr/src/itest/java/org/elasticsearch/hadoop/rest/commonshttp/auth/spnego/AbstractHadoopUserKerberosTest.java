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

package org.elasticsearch.hadoop.rest.commonshttp.auth.spnego;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.hadoop.mr.security.HadoopUser;
import org.elasticsearch.hadoop.security.UgiUtil;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class AbstractHadoopUserKerberosTest {

    private static File KEYTAB_FILE;

    @BeforeClass
    public static void setUp() throws Exception {
        KEYTAB_FILE = KerberosSuite.getKeytabFile();
    }

    @After
    public void resetUGI() {
        UgiUtil.resetUGI();
    }

    @Test
    public void getKerberosPrincipal() throws IOException {
        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Client and Execute Test
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI(KerberosSuite.PRINCIPAL_CLIENT,
                KEYTAB_FILE.getAbsolutePath());

        User user = new HadoopUser(client, new TestSettings());

        assertThat(user.getKerberosPrincipal(), is(not(nullValue())));
        assertThat(user.getKerberosPrincipal().getName(), is(equalTo(KerberosSuite.PRINCIPAL_CLIENT + "@" + KerberosSuite.DEFAULT_REALM)));
    }
}
