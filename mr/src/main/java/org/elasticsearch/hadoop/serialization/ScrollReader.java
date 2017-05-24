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
package org.elasticsearch.hadoop.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.EsHadoopParsingException;
import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.builder.ValueParsingCallback;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils;
import org.elasticsearch.hadoop.serialization.field.FieldFilter;
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Class handling the conversion of data from ES to target objects. It performs tree navigation tied to a potential ES mapping (if available).
 * Expected to read a _search response.
 */
public class ScrollReader {

    private static class JsonFragment {
        static final JsonFragment EMPTY = new JsonFragment(-1, -1) {

            @Override
            public String toString() {
                return "Empty";
            }
        };

        final int charStart, charStop;

        JsonFragment(int charStart, int charStop) {
            this.charStart = charStart;
            this.charStop = charStop;
        }

        boolean isValid() {
            return charStart >= 0 && charStop >= 0;
        }

        @Override
        public String toString() {
            return "[" + charStart + "," + charStop + "]";
        }
    }

    // a collection of Json Fragments
    private static class JsonResult {

        private JsonFragment doc = JsonFragment.EMPTY;

        // typically only 2 fragments are needed = metadata prefix +
        private final List<JsonFragment> fragments = new ArrayList<JsonFragment>(2);

        void addMetadata(JsonFragment fragment) {
            if (fragment != null && fragment.isValid()) {
                this.fragments.add(fragment);
            }
        }

        void addDoc(JsonFragment fragment) {
            if (fragment != null && fragment.isValid()) {
                this.doc = fragment;
            }
        }

        boolean hasDoc() {
            return doc.isValid();
        }

        int[] asCharPos() {
            int positions = fragments.size() << 1;
            if (doc.isValid()) {
                positions += 2;
            }

            int[] pos = new int[positions];
            int index = 0;

            if (doc.isValid()) {
                pos[index++] = doc.charStart;
                pos[index++] = doc.charStop;
            }

            for (JsonFragment fragment : fragments) {
                pos[index++] = fragment.charStart;
                pos[index++] = fragment.charStop;
            }

            return pos;
        }

        @Override
        public String toString() {
            return "doc=" + doc + "metadata=" + fragments;
        }
    }

    public static class Scroll {
        static final Scroll EMPTY = new Scroll("", -1l, Collections.<Object[]> emptyList());
        
        private final String scrollId;
        private final long total;
        private final List<Object[]> hits;

        private Scroll(String scrollId, long total, List<Object[]> hits) {
            this.scrollId = scrollId;
            this.hits = hits;
            this.total = total;
        }

        public String getScrollId() {
            return scrollId;
        }

        public long getTotalHits() {
            return total;
        }

        public List<Object[]> getHits() {
            return hits;
        }
    }

    public static class ScrollReaderConfig {
        public ValueReader reader;

        public boolean readMetadata;
        public String metadataName;
        public boolean returnRawJson;
        public boolean ignoreUnmappedFields;
        public List<String> includeFields;
        public List<String> excludeFields;
        public Field rootField;

        public ScrollReaderConfig(ValueReader reader, Field rootField, boolean readMetadata, String metadataName,
                boolean returnRawJson, boolean ignoreUnmappedFields, List<String> includeFields,
                List<String> excludeFields) {
            super();
            this.reader = reader;
            this.readMetadata = readMetadata;
            this.metadataName = metadataName;
            this.returnRawJson = returnRawJson;
            this.ignoreUnmappedFields = ignoreUnmappedFields;
            this.includeFields = includeFields;
            this.excludeFields = excludeFields;
            this.rootField = rootField;
        }

        public ScrollReaderConfig(ValueReader reader, Field rootField, boolean readMetadata, String metadataName, boolean returnRawJson, boolean ignoreUnmappedFields) {
            this(reader, rootField, readMetadata, metadataName, returnRawJson, ignoreUnmappedFields, Collections.<String> emptyList(), Collections.<String> emptyList());
        }

        public ScrollReaderConfig(ValueReader reader) {
            this(reader, null, false, "_metadata", false, false, Collections.<String> emptyList(), Collections.<String> emptyList());
        }

        public ScrollReaderConfig(ValueReader reader, Field field, Settings cfg) {
            this(reader, field, cfg.getReadMetadata(), cfg.getReadMetadataField(),
                    cfg.getOutputAsJson(), cfg.getReadMappingMissingFieldsIgnore(),
                    StringUtils.tokenize(cfg.getReadFieldInclude()), StringUtils.tokenize(cfg.getReadFieldExclude()));
        }
    }

    private static final Log log = LogFactory.getLog(ScrollReader.class);

    private Parser parser;
    private final ValueReader reader;
    private final ValueParsingCallback parsingCallback;
    private final Map<String, FieldType> esMapping;
    private final boolean trace = log.isTraceEnabled();
    private final boolean readMetadata;
    private final String metadataField;
    private final boolean returnRawJson;
    private final boolean ignoreUnmappedFields;

    private boolean insideGeo = false;

    private final List<NumberedInclude> includeFields;
    private final List<String> excludeFields;

    private static final String[] SCROLL_ID = new String[] { "_scroll_id" };
    private static final String[] HITS = new String[] { "hits" };
    private static final String ID_FIELD = "_id";
    private static final String[] ID = new String[] { ID_FIELD };
    private static final String[] FIELDS = new String[] { "fields" };
    private static final String[] SOURCE = new String[] { "_source" };
    private static final String[] TOTAL = new String[] { "hits", "total" };

    public ScrollReader(ScrollReaderConfig scrollConfig) {
        this.reader = scrollConfig.reader;
        this.parsingCallback = (reader instanceof ValueParsingCallback ?  (ValueParsingCallback) reader : null);

        this.readMetadata = scrollConfig.readMetadata;
        this.metadataField = scrollConfig.metadataName;
        this.returnRawJson = scrollConfig.returnRawJson;
        this.ignoreUnmappedFields = scrollConfig.ignoreUnmappedFields;
        this.includeFields = FieldFilter.toNumberedFilter(scrollConfig.includeFields);
        this.excludeFields = scrollConfig.excludeFields;

        Field mapping = scrollConfig.rootField;
        // optimize filtering
        if (ignoreUnmappedFields) {
            mapping = MappingUtils.filter(mapping, scrollConfig.includeFields, scrollConfig.excludeFields);
        }

        this.esMapping = Field.toLookupMap(mapping);
    }

    public Scroll read(InputStream content) throws IOException {
        Assert.notNull(content);

        BytesArray copy = null;

        if (log.isTraceEnabled() || returnRawJson) {
            //copy content
            copy = IOUtils.asBytes(content);
            content = new FastByteArrayInputStream(copy);
            log.trace("About to parse scroll content " + copy);
        }

        this.parser = new JacksonJsonParser(content);

        try {
            return read(copy);
        } finally {
            parser.close();
        }
    }

    private Scroll read(BytesArray input) {
        // get scroll_id
        Token token = ParsingUtils.seek(parser, SCROLL_ID);
        Assert.isTrue(token == Token.VALUE_STRING, "invalid response");
        String scrollId = parser.text();

        long totalHits = hitsTotal();
        // check hits/total
        if (totalHits == 0) {
            return Scroll.EMPTY;
        }

        // move to hits/hits
        token = ParsingUtils.seek(parser, HITS);

        // move through the list and for each hit, extract the _id and _source
        Assert.isTrue(token == Token.START_ARRAY, "invalid response");

        List<Object[]> results = new ArrayList<Object[]>();

        for (token = parser.nextToken(); token != Token.END_ARRAY; token = parser.nextToken()) {
            results.add(readHit());
        }

        // convert the char positions into actual content
        if (returnRawJson) {
            // get all the longs
            int[] pos = new int[results.size() * 6];
            int offset = 0;

            List<int[]> fragmentsPos = new ArrayList<int[]>(results.size());

            for (Object[] result : results) {
                int[] asCharPos = ((JsonResult) result[1]).asCharPos();
                // remember the positions to easily replace the fragment later on
                fragmentsPos.add(asCharPos);
                // copy them into the lookup array
                System.arraycopy(asCharPos, 0, pos, offset, asCharPos.length);
                offset += asCharPos.length;
            }
            // convert them into byte positions
            //int[] bytesPosition = BytesUtils.charToBytePosition(input, pos);
            int[] bytesPosition = pos;

            int bytesPositionIndex = 0;

            BytesArray doc = new BytesArray(128);
            // replace the fragments with the actual json

            // trimming is currently disabled since it appears mainly within fields and not outside of it
            // in other words in needs to be treated when the fragments are constructed
            for (int fragmentIndex = 0; fragmentIndex < fragmentsPos.size(); fragmentIndex++ ) {

                Object[] result = results.get(fragmentIndex);
                JsonResult jsonPointers = (JsonResult) result[1];

                // current fragment of doc + metadata (prefix + suffix)
                // used to iterate through the byte array pointers
                int[] fragmentPos = fragmentsPos.get(fragmentIndex);
                int currentFragmentIndex = 0;

                int rangeStart, rangeStop;

                doc.add('{');
                // first add the doc
                if (jsonPointers.hasDoc()) {
                    rangeStart = bytesPosition[bytesPositionIndex];
                    rangeStop = bytesPosition[bytesPositionIndex + 1];

                    if (rangeStop - rangeStart < 0) {
                        throw new IllegalArgumentException(String.format("Invalid position given=%s %s",rangeStart, rangeStop));
                    }

                    // trim
                    //rangeStart = BytesUtils.trimLeft(input.bytes(), rangeStart, rangeStop);
                    //rangeStop = BytesUtils.trimRight(input.bytes(), rangeStart, rangeStop);

                    doc.add(input.bytes(), rangeStart, rangeStop - rangeStart);

                    // consumed doc pointers
                    currentFragmentIndex += 2;
                    bytesPositionIndex += 2;

                }
                // followed by the metadata under designed field
                if (readMetadata) {
                    if (jsonPointers.hasDoc()) {
                        doc.add(',');
                    }
                    doc.add('"');
                    doc.add(StringUtils.jsonEncoding(metadataField));
                    doc.add('"');
                    doc.add(':');
                    doc.add('{');

                    // consume metadata
                    for (; currentFragmentIndex < fragmentPos.length; currentFragmentIndex += 2) {
                        rangeStart = bytesPosition[bytesPositionIndex];
                        rangeStop = bytesPosition[bytesPositionIndex + 1];
                        // trim
                        //rangeStart = BytesUtils.trimLeft(input.bytes(), rangeStart, rangeStop);
                        //rangeStop = BytesUtils.trimRight(input.bytes(), rangeStart, rangeStop);

                        if (rangeStop - rangeStart < 0) {
                            throw new IllegalArgumentException(String.format("Invalid position given=%s %s",rangeStart, rangeStop));
                        }

                        doc.add(input.bytes(), rangeStart, rangeStop - rangeStart);
                        bytesPositionIndex += 2;
                    }
                    doc.add('}');
                }
                doc.add('}');

                // replace JsonResult with assembled document
                result[1] = reader.wrapString(doc.toString());
                doc.reset();
            }
        }

        return new Scroll(scrollId, totalHits, results);
    }

    private Object[] readHit() {
        Token t = parser.currentToken();
        Assert.isTrue(t == Token.START_OBJECT, "expected object, found " + t);
        return (returnRawJson ? readHitAsJson() : readHitAsMap());
    }

    private Object[] readHitAsMap() {
        Object[] result = new Object[2];
        Object metadata = null;
        Object id = null;

        Token t = parser.currentToken();

        if (parsingCallback != null) {
            parsingCallback.beginDoc();
        }
        // read everything until SOURCE or FIELDS is encountered
        if (readMetadata) {
            if (parsingCallback != null) {
                parsingCallback.beginLeadMetadata();
            }

            metadata = reader.createMap();
            result[1] = metadata;
            String absoluteName;

            // move parser
            t = parser.nextToken();
            while ((t = parser.currentToken()) != null) {
                String name = parser.currentName();
                absoluteName = StringUtils.stripFieldNameSourcePrefix(parser.absoluteName());
                Object value = null;

                if (t == Token.FIELD_NAME) {
                    if (!("fields".equals(name) || "_source".equals(name))) {
                        reader.beginField(absoluteName);
                        value = read(absoluteName, parser.nextToken(), null);
                        if (ID_FIELD.equals(name)) {
                            id = value;
                        }

                        reader.addToMap(metadata, reader.wrapString(name), value);
                        reader.endField(absoluteName);
                    }
                    else {
                        t = parser.nextToken();
                        break;
                    }
                }
                else {
                    // if = no _source or field found, else select START_OBJECT
                    t = null;
                    break;
                }
            }

            if (parsingCallback != null) {
                parsingCallback.endLeadMetadata();
            }

            Assert.notNull(id, "no id found");
            result[0] = id;
        }
        // no metadata is needed, fast fwd
        else {
            Assert.notNull(ParsingUtils.seek(parser, ID), "no id found");
            result[0] = reader.wrapString(parser.text());
            t = ParsingUtils.seek(parser, SOURCE, FIELDS);
        }

        // no fields found
        Object data = Collections.emptyMap();

        if (t != null) {
            if (parsingCallback != null) {
                parsingCallback.beginSource();
            }

            data = read(StringUtils.EMPTY, t, null);

            if (parsingCallback != null) {
                parsingCallback.endSource();
            }

            if (readMetadata) {
                reader.addToMap(data, reader.wrapString(metadataField), metadata);
            }
        }
        else {
            if (readMetadata) {
                if (parsingCallback != null) {
                    parsingCallback.excludeSource();
                }

                data = reader.createMap();
                reader.addToMap(data, reader.wrapString(metadataField), metadata);
            }
        }

        result[1] = data;

        if (readMetadata) {
            if (parsingCallback != null) {
                parsingCallback.beginTrailMetadata();
            }
        }

        // in case of additional fields (matched_query), add them to the metadata
        while (parser.currentToken() == Token.FIELD_NAME) {
            String name = parser.currentName();
            String absoluteName = StringUtils.stripFieldNameSourcePrefix(parser.absoluteName());
            if (readMetadata) {
                // skip sort (useless and is an array which triggers the row mapping which does not apply)
                if (!"sort".equals(name)) {
                    reader.addToMap(data, reader.wrapString(name), read(absoluteName, parser.nextToken(), null));
                }
                else {
                    parser.nextToken();
                    parser.skipChildren();
                    parser.nextToken();
                }
            }
            else {
                parser.nextToken();
                parser.skipChildren();
                parser.nextToken();
            }
        }

        if (readMetadata) {
            if (parsingCallback != null) {
                parsingCallback.endTrailMetadata();
            }
        }

        if (parsingCallback != null) {
            parsingCallback.endDoc();
        }

        if (trace) {
            log.trace(String.format("Read hit result [%s]", result));
        }

        return result;
    }

    private boolean shouldSkip(String absoluteName) {
        // when parsing geo structures, ignore filtering as depending on the
        // type, JSON can have an object structure
        // especially for geo shapes
        if (insideGeo) {
            return false;
        }
        // if ignoring unmapped fields, the filters are already applied
        if (ignoreUnmappedFields) {
            return !esMapping.containsKey(absoluteName);
        }
        else {
            return !FieldFilter.filter(absoluteName, includeFields, excludeFields).matched;
        }
    }

    private Object[] readHitAsJson() {
        // return results as raw json

        Object[] result = new Object[2];
        Object id = null;

        Token t = parser.currentToken();

        JsonResult snippet = new JsonResult();

        // read everything until SOURCE or FIELDS is encountered
        if (readMetadata) {
            result[1] = snippet;

            String name;
            String absoluteName;

            t = parser.nextToken();
            // move parser
            int metadataStartChar = parser.tokenCharOffset();
            int metadataStopChar = -1;
            int endCharOfLastElement = -1;

            while ((t = parser.currentToken()) != null) {
                name = parser.currentName();
                absoluteName = StringUtils.stripFieldNameSourcePrefix(parser.absoluteName());

                if (t == Token.FIELD_NAME) {
                    if (ID_FIELD.equals(name)) {

                        reader.beginField(absoluteName);

                        t = parser.nextToken();
                        id = reader.wrapString(parser.text());
                        endCharOfLastElement = parser.tokenCharOffset();

                        reader.endField(absoluteName);
                        t = parser.nextToken();
                    }
                    else if ("fields".equals(name) || "_source".equals(name)) {
                        metadataStopChar = endCharOfLastElement;
                        // break meta-parsing
                        t = parser.nextToken();
                        break;
                    }
                    else {
                        parser.skipChildren();
                        parser.nextToken();
                        t = parser.nextToken();
                        endCharOfLastElement = parser.tokenCharOffset();
                    }
                }
                else {
                    // no _source or field found
                    metadataStopChar = endCharOfLastElement;
                    //parser.nextToken();
                    // indicate no data found
                    t = null;
                    break;
                }
            }

            Assert.notNull(id, "no id found");
            result[0] = id;

            if (metadataStartChar >= 0 && metadataStopChar >= 0) {
                snippet.addMetadata(new JsonFragment(metadataStartChar, metadataStopChar));
            }
        }
        // no metadata is needed, fast fwd
        else {
            Assert.notNull(ParsingUtils.seek(parser, ID), "no id found");

            String absoluteName = StringUtils.stripFieldNameSourcePrefix(parser.absoluteName());
            reader.beginField(absoluteName);
            result[0] = reader.wrapString(parser.text());
            reader.endField(absoluteName);

            t = ParsingUtils.seek(parser, SOURCE, FIELDS);
        }

        // no fields found
        if (t != null) {
            // move past _source or fields field name to get the accurate token location
            t = parser.nextToken();
            switch (t) {
                case FIELD_NAME:
                    int charStart = parser.tokenCharOffset();
                    // can't use skipChildren as we are within the object
                ParsingUtils.skipCurrentBlock(parser);
                    // make sure to include the ending char
                    int charStop = parser.tokenCharOffset();
                    // move pass end of object
                    t = parser.nextToken();
                    snippet.addDoc(new JsonFragment(charStart, charStop));
                    break;
                case END_OBJECT:
                    // move pass end of object
                    t = parser.nextToken();
                    snippet.addDoc(JsonFragment.EMPTY);
                    break;
                default:
                    throw new EsHadoopIllegalArgumentException("unexpected token in _source: " + t);
            }
        }

        // should include , plus whatever whitespace there is
        int metadataSuffixStartCharPos = parser.tokenCharOffset();
        int metadataSuffixStopCharPos = -1;

        // in case of additional fields (matched_query), add them to the metadata
        while ((t = parser.currentToken()) == Token.FIELD_NAME) {
            t = parser.nextToken();
            ParsingUtils.skipCurrentBlock(parser);
            t = parser.nextToken();

            if (readMetadata) {
                metadataSuffixStopCharPos = parser.tokenCharOffset();
            }
        }

        if (readMetadata) {
            if (metadataSuffixStartCharPos >= 0 && metadataSuffixStopCharPos >= 0) {
                snippet.addMetadata(new JsonFragment(metadataSuffixStartCharPos, metadataSuffixStopCharPos));
            }
        }

        result[1] = snippet;

        if (trace) {
            log.trace(String.format("Read hit result [%s]", result));
        }

        return result;

    }

    private long hitsTotal() {
        ParsingUtils.seek(parser, TOTAL);
        long hits = parser.longValue();
        return hits;
    }

    protected Object read(String fieldName, Token t, String fieldMapping) {
        if (t == Token.START_ARRAY) {
            return list(fieldName, fieldMapping);
        }

        // handle nested nodes first
        else if (t == Token.START_OBJECT) {
            return map(fieldMapping);
        }
        FieldType esType = mapping(fieldMapping);

        if (t.isValue()) {
            String rawValue = parser.text();
            try {
                return parseValue(esType);
            } catch (Exception ex) {
                throw new EsHadoopParsingException(String.format(Locale.ROOT, "Cannot parse value [%s] for field [%s]", rawValue, fieldName), ex);
            }
        }
        return null;
    }

    private Object parseValue(FieldType esType) {
        Object obj;
        // special case of handing null (as text() will return "null")
        if (parser.currentToken() == Token.VALUE_NULL) {
            obj = null;
        }
        else {
            obj = reader.readValue(parser, parser.text(), esType);
        }
        parser.nextToken();
        return obj;
    }

    protected Object list(String fieldName, String fieldMapping) {
        Token t = parser.currentToken();

        if (t == null) {
            t = parser.nextToken();
        }
        if (t == Token.START_ARRAY) {
            t = parser.nextToken();
        }

        Object array = reader.createArray(mapping(fieldMapping));
        // create only one element since with fields, we always get arrays which create unneeded allocations
        List<Object> content = new ArrayList<Object>(1);
        for (; parser.currentToken() != Token.END_ARRAY;) {
            content.add(read(fieldName, parser.currentToken(), fieldMapping));
        }

        // eliminate END_ARRAY
        parser.nextToken();

        array = reader.addToArray(array, content);
        return array;
    }

    protected Object map(String fieldMapping) {
        Token t = parser.currentToken();

        if (t == null) {
            t = parser.nextToken();
        }
        if (t == Token.START_OBJECT) {
            t = parser.nextToken();
        }

        boolean toggleGeo = false;

        if (fieldMapping != null) {
            // parse everything underneath without mapping
            if (FieldType.isGeo(mapping(fieldMapping))) {
                toggleGeo = true;
                insideGeo = true;
                if (parsingCallback != null) {
                    parsingCallback.beginGeoField();
                }
            }
        }
        Object map = reader.createMap();

        for (; parser.currentToken() != Token.END_OBJECT;) {
            String currentName = parser.currentName();
            String nodeMapping = fieldMapping;

            if (nodeMapping != null) {
                nodeMapping = fieldMapping + "." + currentName;
            }
            else {
                nodeMapping = currentName;
            }

            String absoluteName = StringUtils.stripFieldNameSourcePrefix(parser.absoluteName());

            if (!absoluteName.equals(nodeMapping)) {
                throw new EsHadoopParsingException("Different node mapping " + absoluteName + "|" + nodeMapping);
            }

            if (shouldSkip(absoluteName)) {
                Token nt = parser.nextToken();
                if (nt.isValue()) {
                    // consume and move on
                    parser.nextToken();
                }
                else {
                    ParsingUtils.skipCurrentBlock(parser);
                    parser.nextToken();
                }
            }
            else {
                reader.beginField(absoluteName);

                // Must point to field name
                Object fieldName = reader.readValue(parser, currentName, FieldType.STRING);
                // And then the value...
                reader.addToMap(map, fieldName, read(absoluteName, parser.nextToken(), nodeMapping));
                reader.endField(absoluteName);
            }
        }

        // geo field finished, returning
        if (toggleGeo) {
            insideGeo = false;
            if (parsingCallback != null) {
                parsingCallback.endGeoField();
            }
        }

        // eliminate END_OBJECT
        parser.nextToken();

        return map;
    }

    private FieldType mapping(String fieldMapping) {
        FieldType esType = esMapping.get(fieldMapping);

        if (esType != null) {
            return esType;
        }

        // fall back to JSON
        Token currentToken = parser.currentToken();
        if (!currentToken.isValue()) {
            // nested type
            return FieldType.OBJECT;
        }

        switch (currentToken) {
        case VALUE_NULL:
            esType = FieldType.NULL;
            break;
        case VALUE_BOOLEAN:
            esType = FieldType.BOOLEAN;
            break;
        case VALUE_STRING:
            esType = FieldType.STRING;
            break;
        case VALUE_NUMBER:
            NumberType numberType = parser.numberType();
            switch (numberType) {
            case INT:
                esType = FieldType.INTEGER;
                break;
            case LONG:
                esType = FieldType.LONG;
                break;
            case FLOAT:
                esType = FieldType.FLOAT;
                break;
            case DOUBLE:
                esType = FieldType.DOUBLE;
                break;
            case BIG_DECIMAL:
                throw new UnsupportedOperationException();
            case BIG_INTEGER:
                throw new UnsupportedOperationException();
            default:
                break;
            }
            break;
        default:
            break;
        }
        return esType;
    }
}