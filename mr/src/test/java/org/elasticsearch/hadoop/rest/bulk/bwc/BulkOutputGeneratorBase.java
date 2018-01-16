package org.elasticsearch.hadoop.rest.bulk.bwc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Charsets;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.bulk.BulkOutputGenerator;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.serialization.json.JsonFactory;
import org.elasticsearch.hadoop.serialization.json.ObjectReader;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;

public abstract class BulkOutputGeneratorBase implements BulkOutputGenerator {

    private static final String TOOK = "$TOOK$";
    private static final String ERRS = "$ERRS$";
    private static final String OP = "$OP$";
    private static final String IDX = "$IDX$";
    private static final String TYPE = "$TYPE$";
    private static final String ID = "$ID$";
    private static final String VER = "$VER$";
    private static final String STAT = "$STAT$";
    private static final String ETYPE = "$ETYPE$";
    private static final String EMESG = "$EMESG$";

    private final String head =
            "{\n" +
                    "  \"took\": $TOOK$,\n" +
                    "  \"errors\": $ERRS$,\n" +
                    "  \"items\": [\n";

    private final String tail =
            "  ]\n" +
                    "}";

    private Resource resource;
    private long took = 0L;
    private boolean errors = false;
    private List<String> items = new ArrayList<String>();

    private final ObjectMapper mapper;

    public BulkOutputGeneratorBase() {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        mapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }

    protected String getHead() {
        return head;
    }

    protected String getTail() {
        return tail;
    }

    protected abstract String getSuccess();

    protected abstract String getFailure();

    protected abstract Integer getRejectedStatus();

    protected abstract String getRejectionType();

    protected abstract String getRejectionMsg();

    @Override
    public BulkOutputGenerator setInfo(Resource resource, long took) {
        this.resource = resource;
        this.took = took;
        return this;
    }

    @Override
    public BulkOutputGenerator addSuccess(String operation, int status) {
        Assert.notNull(resource);
        items.add(getSuccess()
                .replace(OP, operation)
                .replace(IDX, resource.index())
                .replace(TYPE, resource.type())
                .replace(ID, UUID.randomUUID().toString())
                .replace(VER, "1")
                .replace(STAT, "201")
        );
        return this;
    }

    @Override
    public BulkOutputGenerator addFailure(String operation, int status, String type, String errorMessage) {
        Assert.notNull(resource);
        errors = true;
        items.add(getFailure()
                .replace(OP, operation)
                .replace(IDX, resource.index())
                .replace(TYPE, resource.type())
                .replace(ID, UUID.randomUUID().toString())
                .replace(STAT, Integer.toString(status))
                .replace(ETYPE, type)
                .replace(EMESG, errorMessage)
        );
        return this;
    }

    @Override
    public BulkOutputGenerator addRejection(String operation) {
        Assert.notNull(resource);
        errors = true;
        items.add(getFailure()
                .replace(OP, operation)
                .replace(IDX, resource.index())
                .replace(TYPE, resource.type())
                .replace(ID, UUID.randomUUID().toString())
                .replace(STAT, Integer.toString(getRejectedStatus()))
                .replace(ETYPE, getRejectionType())
                .replace(EMESG, getRejectionMsg())
        );
        return this;
    }

    @Override
    public RestClient.BulkActionResponse generate() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(getHead()
                .replace(TOOK, Long.toString(took))
                .replace(ERRS, Boolean.toString(errors))
        );
        boolean first = true;
        for (String item : items) {
            if (!first) {
                sb.append(",\n");
            }
            sb.append(item);
            first = false;
        }
        sb.append("\n").append(getTail());
        byte[] bytes = sb.toString().getBytes(Charsets.UTF_8);

        Iterator<Map> entries;

        ObjectReader r = JsonFactory.objectReader(mapper, Map.class);
        JsonParser parser = mapper.getJsonFactory().createJsonParser(new FastByteArrayInputStream(bytes));
        if (ParsingUtils.seek(new JacksonJsonParser(parser), "items") == null) {
            entries = Collections.<Map>emptyList().iterator();
        } else {
            entries = r.readValues(parser);
        }

        resource = null;
        took = 0L;
        errors = false;
        items.clear();

        return new RestClient.BulkActionResponse(entries, 200, took);
    }
}
