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

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.util.ObjectUtils;

/**
 * Provides a platform independent way of retrieving the currently running user.
 */
public abstract class UserProvider implements SettingsAware {

    public static UserProvider create(Settings settings) {
        String className = settings.getSecurityUserProviderClass();
        if (className == null) {
            throw new EsHadoopIllegalArgumentException("Could not locate classname for UserProvider. One must be set with " +
                    ConfigurationOptions.ES_SECURITY_USER_PROVIDER_CLASS);
        }
        return ObjectUtils.instantiate(className, settings);
    }

    protected Settings settings;

    protected Settings getSettings() {
        return settings;
    }

    @Override
    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public boolean isEsKerberosEnabled() {
        return settings.getSecurityAuthenticationMethod().equals(AuthenticationMethod.KERBEROS);
    }

    public abstract User getUser();
}
