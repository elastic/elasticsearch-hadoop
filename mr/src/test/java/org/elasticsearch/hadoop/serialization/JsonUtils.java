package org.elasticsearch.hadoop.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.hadoop.rest.EsHadoopParsingException;

/**
 * A set of utilities to parse JSON in tests, the same way that the RestClient might parse json data.
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        MAPPER.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }

    private JsonUtils() { }

    public static Map<String, Object> asMap(InputStream inputStream) {
        Map<String, Object> map;
        try {
            // create parser manually to lower Jackson requirements
            JsonParser jsonParser = MAPPER.getJsonFactory().createJsonParser(inputStream);
            map = MAPPER.readValue(jsonParser, Map.class);
        } catch (IOException ex) {
            throw new EsHadoopParsingException(ex);
        }
        return map;
    }
}
