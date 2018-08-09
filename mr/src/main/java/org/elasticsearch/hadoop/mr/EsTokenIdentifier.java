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

package org.elasticsearch.hadoop.mr;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.security.EsToken;

public class EsTokenIdentifier extends AbstractDelegationTokenIdentifier {

    public static final Text KIND_NAME = new Text("ELASTICSEARCH_AUTH_TOKEN");

    @Override
    public Text getKind() {
        return KIND_NAME;
    }

    public static class Renewer extends TokenRenewer {
        @Override
        public boolean handleKind(Text kind) {
            return KIND_NAME.equals(kind);
        }

        @Override
        public boolean isManaged(Token<?> token) throws IOException {
            return true;
        }

        @Override
        public long renew(Token<?> token, Configuration conf) throws IOException, InterruptedException {
            if (!KIND_NAME.equals(token.getKind())) {
                throw new IOException("Could not renew token of invalid type [" + token.getKind().toString() + "]");
            }
            EsToken esToken = new EsToken(new DataInputStream(new ByteArrayInputStream(token.getPassword())));
            Settings settings = HadoopSettingsManager.loadFrom(conf);
            RestClient client = null;
            try {
                client = createClient(settings);
                // We get the time starting right before we submit the request to have a safer expiration time.
                long startTime = System.currentTimeMillis();
                EsToken refreshedToken = client.refreshToken(esToken); // TODO: Test after ES supports immutable token refresh
                return startTime + refreshedToken.getExpiresIn();
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }

        @Override
        public void cancel(Token<?> token, Configuration conf) throws IOException, InterruptedException {
            if (!KIND_NAME.equals(token.getKind())) {
                throw new IOException("Could not renew token of invalid type [" + token.getKind().toString() + "]");
            }
            EsToken esToken = new EsToken(new DataInputStream(new ByteArrayInputStream(token.getPassword())));
            Settings settings = HadoopSettingsManager.loadFrom(conf);
            RestClient client = null;
            try {
                client = createClient(settings);
                client.cancelToken(esToken);
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }

        protected RestClient createClient(Settings settings) {
            return new RestClient(settings);
        }
    }
}
