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

package org.elasticsearch.hadoop.rest.bulk.bwc;

public class BulkOutputGeneratorV1 extends BulkOutputGeneratorBase {

    @Override
    protected String getSuccess() {
        return "    {\n" +
        "      \"$OP$\" : {\n" +
        "        \"_index\" : \"$IDX$\",\n" +
        "        \"_type\" : \"$TYPE$\",\n" +
        "        \"_id\" : \"$ID$\",\n" +
        "        \"_version\" : $VER$,\n" +
        "        \"_shards\" : {\n" +
        "          \"total\" : 2,\n" +
        "          \"successful\" : 1,\n" +
        "          \"failed\" : 0\n" +
        "        },\n" +
        "        \"status\" : $STAT$\n" +
        "      }\n" +
        "    }";
    }

    @Override
    protected String getFailure() {
        return "    {\n" +
        "      \"$OP$\" : {\n" +
        "        \"_index\" : \"$IDX$\",\n" +
        "        \"_type\" : \"$TYPE$\",\n" +
        "        \"_id\" : \"$ID$\",\n" +
        "        \"status\" : $STAT$,\n" +
        "        \"error\" : \"$EMESG$\"\n" +
        "      }\n" +
        "    }";
    }

    @Override
    protected Integer getRejectedStatus() {
        return 429;
    }

    @Override
    protected String getRejectionType() {
        return "";
    }

    @Override
    protected String getRejectionMsg() {
        return "EsRejectedExecutionException[rejected execution (queue capacity 1) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@22c141ba]";
    }
}
