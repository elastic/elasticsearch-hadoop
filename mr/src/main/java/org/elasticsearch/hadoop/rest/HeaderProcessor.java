package org.elasticsearch.hadoop.rest;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_NET_HTTP_HEADER_PREFIX;

/**
 * Pulls HTTP header information from Configurations, validates them, joins them with connector defaults, and
 * applies them to HTTP Operations.
 */
public final class HeaderProcessor {

    private static final Log LOG = LogFactory.getLog(HeaderProcessor.class);

    /**
     * Headers that are reserved and should not be specified from outside of the connector code.
     */
    private enum ReservedHeaders {
        /**
         * Content-Type HTTP Header should not be set by user as all requests made to Elasticsearch are done
         * in JSON format, or the _bulk api compliant line delimited JSON.
         */
        CONTENT_TYPE("Content-Type", "application/json", "ES-Hadoop communicates in JSON format only"),
        /*
         * Elasticsearch also supports line-delimited JSON for '_bulk' operations which isn't
         * official and has a few variations:
         *  * http://specs.okfnlabs.org/ndjson/
         *  * https://github.com/ndjson/ndjson-spec/blob/48ea03cea6796b614cfbff4d4eb921f0b1d35c26/specification.md
         *
         * The '_bulk' operation will still work with "application/json", but as a note, the suggested MIME types
         * for line delimited json are:
         *  * "application/x-ldjson"
         *  * "application/x-ndjson"
         */
        /**
         * Accept HTTP Header should not be set by user as all responses from Elasticsearch are expected to be
         * in JSON format.
         */
        ACCEPT("Accept", "application/json", "ES-Hadoop communicates in JSON format only");

        private static final Map<String, ReservedHeaders> NAME_MAP = new HashMap<String, ReservedHeaders>();
        static {
            for (ReservedHeaders reservedHeaders : ReservedHeaders.values()) {
                NAME_MAP.put(reservedHeaders.name, reservedHeaders);
            }
        }

        /**
         * @return Map of ReservedHeaders, keyed by header name.
         */
        public static Map<String, ReservedHeaders> byName() {
            return NAME_MAP;
        }

        private final String name;
        private final String defaultValue;
        private final String reasonReserved;

        ReservedHeaders(String name, String defaultValue, String reasonReserved) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.reasonReserved = reasonReserved;
        }

        public String getName() {
            return name;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public String getReasonReserved() {
            return reasonReserved;
        }

        @Override
        public String toString() {
            return "ReservedHeaders{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }

    private final List<Header> headers;

    public HeaderProcessor(Settings settings) {
        Map<String, String> workingHeaders = new HashMap<String, String>();

        for (Map.Entry<Object, Object> prop : settings.asProperties().entrySet()) {
            String key = prop.getKey().toString();

            if (key.startsWith(ES_NET_HTTP_HEADER_PREFIX)) {
                String headerName = key.substring(ES_NET_HTTP_HEADER_PREFIX.length());
                validateName(headerName, prop);
                ensureNotReserved(headerName, workingHeaders);
                workingHeaders.put(headerName, extractHeaderValue(prop.getValue()));
            }
        }

        this.headers = new ArrayList<Header>(workingHeaders.keySet().size());
        for (Map.Entry<String, String> headerData : workingHeaders.entrySet()) {
            headers.add(new Header(headerData.getKey(), headerData.getValue()));
        }
        for (ReservedHeaders reservedHeaders : ReservedHeaders.values()) {
            headers.add(new Header(reservedHeaders.getName(), reservedHeaders.getDefaultValue()));
        }
    }

    private void validateName(String headerName, Map.Entry<Object, Object> property) {
        if (!StringUtils.hasText(headerName)){
            throw new EsHadoopIllegalArgumentException(String.format(
                    "Found configuration entry denoting a header prefix, but no header was given after the prefix. " +
                            "Please add a header name for the configuration entry : [%s] = [%s]",
                    property.getKey(),
                    property.getValue()
            ));
        }
    }

    private void ensureNotReserved(String headerName, Map<String, String> existingHeaders) {
        if (existingHeaders.containsKey(headerName)) {
            throw new EsHadoopIllegalArgumentException(String.format(
                    "Could not set header [%s]: Header value [%s] is already present from a previous setting. " +
                            "Please submit multiple header values as a comma (,) separated list on the property" +
                            "value.",
                    headerName,
                    existingHeaders.get(headerName)
            ));
        }

        if (ReservedHeaders.byName().containsKey(headerName)) {
            throw new EsHadoopIllegalArgumentException(String.format(
                    "Could not set header [%s]: This header is a reserved header. Reason: %s.",
                    headerName,
                    ReservedHeaders.byName().get(headerName).getReasonReserved()
            ));
        }
    }

    private String extractHeaderValue(Object object) {
        String value;
        if (object instanceof Object[]) {
            StringBuilder sb = new StringBuilder();
            for (Object o : (Object[]) object) {
                sb.append(o.toString()).append(',');
            }
            value = sb.substring(0, sb.length() - 1);
        } else {
            value = object.toString();
        }
        return value;
    }

    public HttpMethod applyTo(HttpMethod method) {
        // Add headers to the request.
        for (Header header : headers) {
            method.setRequestHeader(header);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Added HTTP Headers to method: " + Arrays.toString(method.getRequestHeaders()));
        }

        return method;
    }
}
