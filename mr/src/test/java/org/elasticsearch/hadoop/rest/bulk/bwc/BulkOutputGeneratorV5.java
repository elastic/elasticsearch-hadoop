package org.elasticsearch.hadoop.rest.bulk.bwc;

/**
 * BulkOutputGenerator for version 5 of Elasticsearch.
 */
public class BulkOutputGeneratorV5 extends BulkOutputGeneratorBase {

    @Override
    protected String getSuccess() {
        return "    {\n" +
        "      \"$OP$\": {\n" +
        "        \"_index\": \"$IDX$\",\n" +
        "        \"_type\": \"$TYPE$\",\n" +
        "        \"_id\": \"$ID$\",\n" +
        "        \"_version\": $VER$,\n" +
        "        \"forced_refresh\": false,\n" +
        "        \"_shards\": {\n" +
        "          \"total\": 1,\n" +
        "          \"successful\": 1,\n" +
        "          \"failed\": 0\n" +
        "        },\n" +
        "        \"created\": true,\n" +
        "        \"status\": $STAT$\n" +
        "      }\n" +
        "    }";
    }

    @Override
    protected String getFailure() {
        return "    {\n" +
        "      \"$OP$\": {\n" +
        "        \"_index\": \"$IDX$\",\n" +
        "        \"_type\": \"$TYPE$\",\n" +
        "        \"_id\": \"$ID$\",\n" +
        "        \"status\": $STAT$,\n" +
        "        \"error\": {\n" +
        "          \"type\": \"$ETYPE$\",\n" +
        "          \"reason\": \"$EMESG$\"\n" +
        "        }\n" +
        "      }\n" +
        "    }";
    }

    @Override
    protected Integer getRejectedStatus() {
        return 429;
    }

    @Override
    protected String getRejectionType() {
        return "es_rejected_execution_exception";
    }

    @Override
    protected String getRejectionMsg() {
        return "rejected execution of org.elasticsearch.transport.TransportService$5@2c37e0bc on EsThreadPoolExecutor[bulk, queue capacity = 0, org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor@7e1fa465[Running, pool size = 1, active threads = 0, queued tasks = 0, completed tasks = 1]]";
    }
}
