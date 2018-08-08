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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

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
            // TODO
            return 0;
        }

        @Override
        public void cancel(Token<?> token, Configuration conf) throws IOException, InterruptedException {
            // TODO
        }
    }
}
