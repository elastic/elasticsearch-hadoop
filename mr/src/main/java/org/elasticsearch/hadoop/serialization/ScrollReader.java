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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.EsHadoopParsingException;
import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.builder.ValueParsingCallback;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.elasticsearch.hadoop.serialization.field.FieldFilter;
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.read.DeserializationFailure;
import org.elasticsearch.hadoop.serialization.handler.SerdeErrorCollector;
import org.elasticsearch.hadoop.serialization.handler.read.IDeserializationErrorHandler;
import org.elasticsearch.hadoop.serialization.json.BlockAwareJsonParser;
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
public class ScrollReader implements Closeable {

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
        static Scroll empty(String scrollId) {
            return new Scroll(scrollId, 0L, true);
        }

        private final String scrollId;
        private final long total;
        private final List<Object[]> hits;
        private final boolean concluded;
        private final int numberOfHits;
        private final int numberOfSkippedHits;

        public Scroll(String scrollId, long total, boolean concluded) {
            this.scrollId = scrollId;
            this.total = total;
            this.hits = Collections.emptyList();
            this.concluded = concluded;
            this.numberOfHits = 0;
            this.numberOfSkippedHits = 0;
        }

        public Scroll(String scrollId, long total, List<Object[]> hits, int responseHits, int skippedHits) {
            this.scrollId = scrollId;
            this.hits = hits;
            this.total = total;
            this.concluded = false;
            this.numberOfHits = responseHits;
            this.numberOfSkippedHits = skippedHits;
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

        public boolean isConcluded() {
            return concluded;
        }

        public int getNumberOfHits() {
            return numberOfHits;
        }

        public int getNumberOfSkippedHits() {
            return numberOfSkippedHits;
        }
    }

    private static final Log log = LogFactory.getLog(ScrollReader.class);

    private final ValueReader reader;
    private final ValueParsingCallback parsingCallback;
    private final Map<String, FieldType> esMapping;
    private final boolean trace = log.isTraceEnabled();
    private final boolean readMetadata;
    private boolean inMetadataSection;
    private final String metadataField;
    private final boolean returnRawJson;
    private final boolean ignoreUnmappedFields;

    private boolean insideGeo = false;

    private final List<NumberedInclude> includeFields;
    private final List<String> excludeFields;
    private final List<NumberedInclude> includeArrayFields;
    private List<IDeserializationErrorHandler> deserializationErrorHandlers;

    private static final String[] SCROLL_ID = new String[] { "_scroll_id" };
    private static final String[] HITS = new String[] { "hits" };
    private static final String ID_FIELD = "_id";
    private static final String[] ID = new String[] { ID_FIELD };
    private static final String[] FIELDS = new String[] { "fields" };
    private static final String[] SOURCE = new String[] { "_source" };
    private static final String[] TOTAL = new String[] { "hits", "total" };

    public ScrollReader(ScrollReaderConfigBuilder scrollConfig) {
        this.reader = scrollConfig.getReader();
        this.parsingCallback = (reader instanceof ValueParsingCallback ?  (ValueParsingCallback) reader : null);

        this.readMetadata = scrollConfig.getReadMetadata();
        this.metadataField = scrollConfig.getMetadataName();
        this.returnRawJson = scrollConfig.getReturnRawJson();
        this.ignoreUnmappedFields = scrollConfig.getIgnoreUnmappedFields();
        this.includeFields = FieldFilter.toNumberedFilter(scrollConfig.getIncludeFields());
        this.excludeFields = scrollConfig.getExcludeFields();
        this.includeArrayFields = FieldFilter.toNumberedFilter(scrollConfig.getIncludeArrayFields());

        Mapping mapping = scrollConfig.getResolvedMapping();
        if (mapping != null) {
            // optimize filtering
            if (ignoreUnmappedFields) {
                mapping = mapping.filter(scrollConfig.getIncludeFields(), scrollConfig.getExcludeFields());
            }
            this.esMapping = mapping.flatten();
        } else {
            this.esMapping = Collections.emptyMap();
        }

        this.deserializationErrorHandlers = scrollConfig.getErrorHandlerLoader().loadHandlers();
    }

    public Scroll read(InputStream content) throws IOException {
        Assert.notNull(content);

        //copy content
        BytesArray copy = IOUtils.asBytes(content);
        content = new FastByteArrayInputStream(copy);

        if (log.isTraceEnabled()) {
            log.trace("About to parse scroll content " + copy);
        }

        Parser parser = new JacksonJsonParser(content);

        try {
            return read(parser, copy);
        } finally {
            parser.close();
        }
    }

    private Scroll read(Parser parser, BytesArray input) {
        // get scroll_id
        Token token = ParsingUtils.seek(parser, SCROLL_ID);
        if (token == null) { // no scroll id is returned for frozen indices
            if (log.isTraceEnabled()) {
                log.info("No scroll id found, likely because the index is frozen");
            }
            return null;
        }
        Assert.isTrue(token == Token.VALUE_STRING, "invalid response");
        String scrollId = parser.text();

        long totalHits = hitsTotal(parser);
        // check hits/total
        if (totalHits == 0) {
            return Scroll.empty(scrollId);
        }

        // move to hits/hits
        token = ParsingUtils.seek(parser, HITS);

        // move through the list and for each hit, extract the _id and _source
        Assert.isTrue(token == Token.START_ARRAY, "invalid response");

        List<Object[]> results = new ArrayList<Object[]>();
        int responseHits = 0;
        int skippedHits = 0;
        int readHits = 0;
        for (token = parser.nextToken(); token != Token.END_ARRAY; token = parser.nextToken()) {
            responseHits++;
            Object[] hit = readHit(parser, input);
            if (hit != null) {
                readHits++;
                results.add(hit);
            } else {
                skippedHits++;
            }
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

        if (responseHits > 0) {
            return new Scroll(scrollId, totalHits, results, responseHits, skippedHits);
        } else {
            // Scroll had no hits in the response, it must have concluded.
            return new Scroll(scrollId, totalHits, true);
        }
    }

    private Object[] readHit(Parser parser, BytesArray input) {
        Token t = parser.currentToken();
        Assert.isTrue(t == Token.START_OBJECT, "expected object, found " + t);
        int hitStartPos = parser.tokenCharOffset();
        // Wrap the parser in a block aware parser so we can skip a hit if its parsing fails.
        BlockAwareJsonParser blockAwareJsonParser = new BlockAwareJsonParser(parser);
        // This is the parser that we will be using to parse a hit. Starts with the block parser and the main scroll,
        // but may change during the course of the function if hits are retried with different parsers.
        Parser workingParser = blockAwareJsonParser;

        Object[] readResult = null;
        boolean retryRead = false;
        boolean skip = false;
        int attempts = 0;
        do {
            try {
                // Clear the retry flag
                retryRead = false;
                if (returnRawJson) {
                    readResult = readHitAsJson(workingParser);
                } else {
                    readResult = readHitAsMap(workingParser);
                }
            } catch (Exception deserializationException) {
                // Skip this hit if we need to
                if (workingParser == blockAwareJsonParser) {
                    blockAwareJsonParser.exitBlock();
                    t = blockAwareJsonParser.currentToken();
                    Assert.isTrue(t == Token.END_OBJECT, "expected end of object, found " + t);
                }

                // slice input data to create an input stream for the handler event
                int hitEndPos = parser.tokenCharOffset();
                BytesArray hitSection = new BytesArray(input.bytes(), hitStartPos, hitEndPos - hitStartPos + 1);

                // Make error event
                List<String> passReasons = new ArrayList<String>();
                DeserializationFailure event = new DeserializationFailure(deserializationException, hitSection, passReasons);

                // Set up error collector
                SerdeErrorCollector<byte[]> errorCollector = new SerdeErrorCollector<byte[]>();

                // Attempt failure handling
                retryRead = false;
                Exception abortException = deserializationException;
                handlerLoop:
                for (IDeserializationErrorHandler deserializationErrorHandler : deserializationErrorHandlers) {
                    HandlerResult result;
                    try {
                        result = deserializationErrorHandler.onError(event, errorCollector);
                    } catch (EsHadoopAbortHandlerException ahe) {
                        // Count this as an abort operation. Translate the exception into a parsing exception.
                        result = HandlerResult.ABORT;
                        abortException = new EsHadoopParsingException(ahe.getMessage(), ahe.getCause());
                    } catch (Exception e) {
                        // Log the error we're handling before we throw for the handler being broken.
                        log.error("Could not handle deserialization error event due to an exception in error handler. " +
                                "Deserialization exception:", deserializationException);
                        throw new EsHadoopException("Encountered unexpected exception during error handler execution.", e);
                    }

                    switch (result) {
                        case HANDLED:
                            Assert.isTrue(errorCollector.getAndClearMessage() == null,
                                    "Found pass message with Handled response. Be sure to return the value returned from " +
                                            "the pass(String) call.");
                            // Check for retries
                            if (errorCollector.receivedRetries()) {
                                // Reset the working parser with the retry buffer.
                                byte[] retryDataBuffer = errorCollector.getAndClearRetryValue();
                                if (retryDataBuffer == null || hitSection.bytes() == retryDataBuffer) {
                                    // Same buffer as the scroll content.
                                    workingParser = new JacksonJsonParser(event.getHitContents());
                                } else {
                                    // Brand new byte array to use.
                                    workingParser = new JacksonJsonParser(retryDataBuffer);
                                }

                                // Limit the number of retries though to like 50
                                if (attempts >= 50) {
                                    throw new EsHadoopException("Maximum retry attempts (50) reached for deserialization errors.");
                                } else {
                                    retryRead = true;
                                    // Advance to the first token, as it will be expected to be on a start object.
                                    workingParser.nextToken();
                                    attempts++;
                                }
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("Skipping a scroll search hit that resulted in error while reading: [" +
                                            StringUtils.asUTFString(hitSection.bytes(), hitSection.offset(),
                                                    hitSection.length()) + "]");
                                } else {
                                    log.info("Skipping a scroll search hit that resulted in error while reading. (DEBUG for more info).");
                                }
                                skip = true;
                            }
                            break handlerLoop;
                        case PASS:
                            String reason = errorCollector.getAndClearMessage();
                            if (reason != null) {
                                passReasons.add(reason);
                            }
                            continue handlerLoop;
                        case ABORT:
                            errorCollector.getAndClearMessage(); // Sanity clearing
                            if (abortException instanceof EsHadoopParsingException) {
                                throw (EsHadoopParsingException) abortException;
                            } else {
                                throw new EsHadoopParsingException(abortException);
                            }
                    }
                }
            }
        } while (retryRead);

        if (readResult == null && skip == false) {
            throw new EsHadoopParsingException("Could not read hit from scroll response.");
        }

        return readResult;
    }

    private Object[] readHitAsMap(Parser parser) {
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
            inMetadataSection = true;

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
                        value = read(absoluteName, parser.nextToken(), null, parser);
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

            inMetadataSection = false;
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

            data = read(StringUtils.EMPTY, t, null, parser);

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
            inMetadataSection = true;
        }

        // in case of additional fields (matched_query), add them to the metadata
        while (parser.currentToken() == Token.FIELD_NAME) {
            String name = parser.currentName();
            String absoluteName = StringUtils.stripFieldNameSourcePrefix(parser.absoluteName());
            if (readMetadata) {
                // skip sort (useless and is an array which triggers the row mapping which does not apply)
                if (!"sort".equals(name)) {
                    reader.addToMap(data, reader.wrapString(name), read(absoluteName, parser.nextToken(), null, parser));
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
            inMetadataSection = false;
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

    private Object[] readHitAsJson(Parser parser) {
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

    private long hitsTotal(Parser parser) {
        ParsingUtils.seek(parser, TOTAL);

        // In ES 7.0, the "hits.total" field changed to be an object
        // with nested field "hits.total.value" holding the number.
        long hits = Long.MIN_VALUE;
        Token token = parser.currentToken();
        if (token == Token.START_OBJECT) {
            String relation = null;
            token = parser.nextToken();
            while (token != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    if ("value".equals(parser.currentName())) {
                        parser.nextToken();
                        hits = parser.longValue();
                    } else if ("relation".equals(parser.currentName())) {
                        parser.nextToken();
                        relation = parser.text();
                    } else {
                        // Skip unknown field
                        parser.nextToken();
                    }
                } else if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                    parser.skipChildren();
                }
                // Next field
                token = parser.nextToken();
            }
            if (relation == null) {
                throw new EsHadoopParsingException("Could not discern relative value of total hits. Response missing [relation] field.");
            } else if (!"eq".equals(relation)) {
                throw new EsHadoopParsingException(
                        String.format("Could not discern exact hit count for search response. Received [%s][%s]",
                                relation,
                                hits
                        )
                );
            }
        } else if (token == Token.VALUE_NUMBER) {
            hits = parser.longValue();
        }

        if (hits == Long.MIN_VALUE) {
            throw new EsHadoopParsingException("Could not locate total number of hits for search result.");
        }

        return hits;
    }

    protected Object read(String fieldName, Token t, String fieldMapping, Parser parser) {
        if (t == Token.START_ARRAY) {
            return list(fieldName, fieldMapping, parser);
        }

        // handle nested nodes first
        else if (t == Token.START_OBJECT) {
            // Check if the object field is a nested object or a field that should be considered an array.
            FieldType esType = mapping(fieldMapping, parser);
            if ((esType != null && esType.equals(FieldType.NESTED)) || isArrayField(fieldMapping)) {
                // If this field has the nested data type, then this object we are
                // about to read is using the abbreviated single value syntax (no array brackets needed for nested fields
                // that only have one nested element.)
                return singletonList(fieldMapping, map(fieldMapping, parser), parser);
            } else {
                return map(fieldMapping, parser);
            }
        }
        FieldType esType = mapping(fieldMapping, parser);

        if (t.isValue()) {
            String rawValue = parser.text();
            try {
                if (isArrayField(fieldMapping)) {
                    Object parsedValue = parseValue(parser, esType);
                    if (parsedValue == null) {
                        return null; //There is not a null element in the array. The array itself is null.
                    } else {
                        return singletonList(fieldMapping, parsedValue, parser);
                    }
                } else {
                    return parseValue(parser, esType);
                }
            } catch (Exception ex) {
                throw new EsHadoopParsingException(String.format(Locale.ROOT, "Cannot parse value [%s] for field [%s]", rawValue, fieldName), ex);
            }
        }
        return null;
    }

    // Same as read(String, Token, String) above, but does not include checking the current field name to see if it's an array.
    protected Object readListItem(String fieldName, Token t, String fieldMapping, Parser parser) {
        if (t == Token.START_ARRAY) {
            return list(fieldName, fieldMapping, parser);
        }

        // handle nested nodes first
        else if (t == Token.START_OBJECT) {
            // Don't need special handling for nested fields since this field is already in an array.
            return map(fieldMapping, parser);
        }
        FieldType esType = mapping(fieldMapping, parser);

        if (t.isValue()) {
            String rawValue = parser.text();
            try {
                return parseValue(parser, esType);
            } catch (Exception ex) {
                throw new EsHadoopParsingException(String.format(Locale.ROOT, "Cannot parse value [%s] for field [%s]", rawValue, fieldName), ex);
            }
        }
        return null;
    }

    private boolean isArrayField(String fieldName) {
        // Test if the current field is marked as an array field in the include array property
        if (fieldName != null) {
            if (includeArrayFields != null && !includeArrayFields.isEmpty()) {
                return FieldFilter.filter(fieldName, includeArrayFields, null, false).matched;
            }
        }
        return false;
    }

    private Object parseValue(Parser parser, FieldType esType) {
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

    protected Object list(String fieldName, String fieldMapping, Parser parser) {
        Token t = parser.currentToken();

        if (t == null) {
            t = parser.nextToken();
        }
        if (t == Token.START_ARRAY) {
            t = parser.nextToken();
        }

        Object array = reader.createArray(mapping(fieldMapping, parser));
        // create only one element since with fields, we always get arrays which create unneeded allocations
        List<Object> content = new ArrayList<Object>(1);
        for (; parser.currentToken() != Token.END_ARRAY;) {
            content.add(readListItem(fieldName, parser.currentToken(), fieldMapping, parser));
        }

        // eliminate END_ARRAY
        parser.nextToken();

        array = reader.addToArray(array, content);
        return array;
    }

    protected Object singletonList(String fieldMapping, Object value, Parser parser) {
        Object array = reader.createArray(mapping(fieldMapping, parser));
        // create only one element since with fields, we always get arrays which create unneeded allocations
        List<Object> content = new ArrayList<Object>(1);
        content.add(value);
        array = reader.addToArray(array, content);
        return array;
    }

    protected Object map(String fieldMapping, Parser parser) {
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
            if (FieldType.isGeo(mapping(fieldMapping, parser))) {
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
                reader.addToMap(map, fieldName, read(absoluteName, parser.nextToken(), nodeMapping, parser));
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

    private FieldType mapping(String fieldMapping, Parser parser) {
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

        // If we're currently parsing a metadata section, return string types for primitive fields
        if (inMetadataSection) {
            return FieldType.STRING;
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

    @Override
    public void close() {
        for (IDeserializationErrorHandler handler : deserializationErrorHandlers) {
            handler.close();
        }
    }
}
