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

package org.elasticsearch.hadoop.security;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class LoginUtil {

    public static LoginContext login(String userPrincipal, String password) throws LoginException {
        Set<Principal> principals = Collections.singleton(new KerberosPrincipal(userPrincipal));
        Subject subject = new Subject(false, principals, Collections.emptySet(), Collections.emptySet());
        Configuration loginConf = new KerberosPasswordConfiguration(userPrincipal);
        CallbackHandler callback = new KerberosPasswordCallbackHandler(userPrincipal, password);
        LoginContext loginContext = new LoginContext(KERBEROS_CONFIG_NAME, subject, callback, loginConf);
        loginContext.login();
        return loginContext;
    }

    public static LoginContext keytabLogin(String userPrincipal, String keytab) throws LoginException {
        Set<Principal> principals = Collections.singleton(new KerberosPrincipal(userPrincipal));
        Subject subject = new Subject(false, principals, Collections.emptySet(), Collections.emptySet());
        Configuration loginConf = new KerberosKeytabConfiguration(userPrincipal, keytab);
        LoginContext loginContext = new LoginContext(KERBEROS_CONFIG_NAME, subject, null, loginConf);
        loginContext.login();
        return loginContext;
    }

    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String KERBEROS_CONFIG_NAME = "es-hadoop-user-kerberos";

    private static class KerberosPasswordConfiguration extends javax.security.auth.login.Configuration {
        private final String principalName;

        public KerberosPasswordConfiguration(String principalName) {
            this.principalName = principalName;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<>();
            options.put("principal", principalName);
            options.put("storeKey", "true");
            options.put("isInitiator", "true");
            options.put("refreshKrb5Config", "true");
            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry(SUN_KRB5_LOGIN_MODULE, LoginModuleControlFlag.REQUIRED, options)
            };
        }
    }

    private static class KerberosPasswordCallbackHandler implements CallbackHandler {
        private final String principalName;
        private final String password;

        public KerberosPasswordCallbackHandler(String principalName, String password) {
            this.principalName = principalName;
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    if (pc.getPrompt().contains(principalName)) {
                        pc.setPassword(password.toCharArray());
                        break;
                    }
                }
            }
        }
    }

    private static class KerberosKeytabConfiguration extends javax.security.auth.login.Configuration {
        private final String principalName;
        private final String keytabFile;

        public KerberosKeytabConfiguration(String principalName, String keytabFile) {
            this.principalName = principalName;
            this.keytabFile = keytabFile;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<>();
            options.put("doNotPrompt", "true");
            options.put("principal", principalName);
            options.put("storeKey", "true");
            options.put("isInitiator", "true");
            options.put("refreshKrb5Config", "true");
            options.put("useKeyTab", "true");
            options.put("keyTab", keytabFile);
            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry(SUN_KRB5_LOGIN_MODULE, LoginModuleControlFlag.REQUIRED, options)
            };
        }
    }
}
