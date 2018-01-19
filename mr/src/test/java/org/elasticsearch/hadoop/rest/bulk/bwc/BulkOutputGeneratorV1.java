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
