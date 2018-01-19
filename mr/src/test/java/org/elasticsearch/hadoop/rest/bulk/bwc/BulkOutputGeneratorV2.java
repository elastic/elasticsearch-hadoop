package org.elasticsearch.hadoop.rest.bulk.bwc;

public class BulkOutputGeneratorV2 extends BulkOutputGeneratorBase {

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
        return "   {\n" +
        "      \"$OP$\" : {\n" +
        "        \"_index\" : \"$IDX$\",\n" +
        "        \"_type\" : \"$TYPE$\",\n" +
        "        \"_id\" : \"$ID$\",\n" +
        "        \"status\" : $STAT$,\n" +
        "        \"error\" : {\n" +
        "          \"type\" : \"$ETYPE$\",\n" +
        "          \"reason\" : \"$EMESG$\"\n" +
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
        return "rejected execution of org.elasticsearch.action.support.replication.TransportReplicationAction$PrimaryPhase$1@4986e6de on EsThreadPoolExecutor[bulk, queue capacity = 1, org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor@421ce219[Running, pool size = 4, active threads = 2, queued tasks = 0, completed tasks = 4]]";
    }
}
