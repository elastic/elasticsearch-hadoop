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

package org.elasticsearch.hadoop.serialization.json;

import java.nio.charset.Charset;

import org.elasticsearch.hadoop.serialization.Parser;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class BlockAwareJsonParserTest {

    /*
     * first ^ : The block aware parser is created on this token
     * ...---^ : The tokens skipped when exitBlock is called, with the ^ pointing to the token stream cursor after the operation.
     * |---... : The location in the token stream that exitBlock is called in this test case if it does not exit right after being created.
     * X : The current token when exitBlock is called, but the exitBlock function does not exit anything.
     */

    @Test
    public void testNoSkipping() {
        String data = "{\"test\":\"value\"}";
        //                 ^X
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("test"));
        BlockAwareJsonParser blockParser = new BlockAwareJsonParser(parser);
        blockParser.exitBlock();
        assertThat(parser.currentToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("test"));
    }

    @Test
    public void testSkipping() {
        String data = "{\"test\":\"value\"}";
        //            ^     |-------------^
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        BlockAwareJsonParser blockParser = new BlockAwareJsonParser(parser);
        assertThat(blockParser.tokenCharOffset(), equalTo(0));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(blockParser.text(), equalTo("test"));
        blockParser.exitBlock();
        assertThat(blockParser.tokenCharOffset(), equalTo(15));
        assertThat(parser.currentToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testSkippingAtStart() {
        String data = "{\"test\":\"value\"}";
        //             ^    |-------------^
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        BlockAwareJsonParser blockParser = new BlockAwareJsonParser(parser);
        assertThat(blockParser.tokenCharOffset(), equalTo(0));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(blockParser.text(), equalTo("test"));
        blockParser.exitBlock();
        assertThat(blockParser.tokenCharOffset(), equalTo(15));
        assertThat(parser.currentToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testSkippingArray() {
        String data = "{\"array\":[{\"test\":\"value\"}]}";
        //                  ^      |-------------------^
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("array"));
        BlockAwareJsonParser blockParser = new BlockAwareJsonParser(parser);
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.START_ARRAY));
//        assertThat(blockParser.tokenCharOffset(), equalTo(9)); // Doesn't quite work correctly?
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(blockParser.tokenCharOffset(), equalTo(10));
        blockParser.exitBlock();
        assertThat(blockParser.tokenCharOffset(), equalTo(26));
        assertThat(parser.currentToken(), equalTo(Parser.Token.END_ARRAY));
        assertThat(parser.nextToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testSkippingAtArrayStart() {
        String data = "{\"array\":[{\"test\":\"value\"}]}";
        //                        ^--------------------^
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("array"));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_ARRAY));
        BlockAwareJsonParser blockParser = new BlockAwareJsonParser(parser);
        blockParser.exitBlock();
        assertThat(parser.currentToken(), equalTo(Parser.Token.END_ARRAY));
        assertThat(parser.nextToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(parser.nextToken(), nullValue());
    }

    @Test
    public void testSkippingOutOfScope() {
        String data = "{\"array\":[{\"test\":\"value\"}, {\"test2\":\"value2\"}]}";
        //                         ^------------------^                        X
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("array"));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_ARRAY));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        BlockAwareJsonParser blockParser = new BlockAwareJsonParser(parser);
        blockParser.exitBlock();
        assertThat(blockParser.currentToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(blockParser.text(), equalTo("test2"));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.VALUE_STRING));
        assertThat(blockParser.text(), equalTo("value2"));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(blockParser.nextToken(), equalTo(Parser.Token.END_ARRAY));
        boolean failed = false;
        try {
            blockParser.exitBlock();
        } catch (Exception e) {
            failed = true;
        }
        if (!failed) {
            fail("Should not have successfully exited a block out of scope of itself.");
        }
    }

    /**
     * In reality this test case is most likely not going to arise unless the underlying parser is used (Jackson
     * defends against invalid JSON already), but it is included for sanity and for code coverage.
     */
    @Test
    public void testSkippingAndEncounterEOF() {
        String data = "{\"array\":[{\"test\":\"value\"}]}";
        //                         ^                    |X
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("array"));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_ARRAY));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        BlockAwareJsonParser blockParser = new BlockAwareJsonParser(parser);
        // Improper use (using underlying parser instead of block parser)
        assertThat(parser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("test"));
        assertThat(parser.nextToken(), equalTo(Parser.Token.VALUE_STRING));
        assertThat(parser.text(), equalTo("value"));
        assertThat(parser.nextToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.END_ARRAY));
        assertThat(parser.nextToken(), equalTo(Parser.Token.END_OBJECT));
        blockParser.exitBlock();
        assertThat(parser.currentToken(), nullValue());
    }

    /**
     * We increment the level of nesting in the parser when getting the next token. If that token starts an array or object,
     * the "open" counter is incremented. If we then call `skipChildren` instead of iterating to the end of the object, make
     * sure that the open counter is decremented.
     */
    @Test
    public void testExitBlockAfterSkippingChildren() {
        String data = "{\"nested\":{\"array\":[\"test\"],\"scalar\":1}}";
        //                         ^          !        |-------------^
        // ! = skipChildren
        Parser parser = new JacksonJsonParser(data.getBytes(Charset.defaultCharset()));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(parser.text(), equalTo("nested"));
        assertThat(parser.nextToken(), equalTo(Parser.Token.START_OBJECT));
        BlockAwareJsonParser blockAwareJsonParser = new BlockAwareJsonParser(parser);
        assertThat(blockAwareJsonParser.getLevel(), equalTo(1));
        assertThat(blockAwareJsonParser.nextToken(), equalTo(Parser.Token.FIELD_NAME));
        assertThat(blockAwareJsonParser.text(), equalTo("array"));
        assertThat(blockAwareJsonParser.nextToken(), equalTo(Parser.Token.START_ARRAY));
        assertThat(blockAwareJsonParser.getLevel(), equalTo(2));
        blockAwareJsonParser.skipChildren();
        assertThat(blockAwareJsonParser.currentToken(), equalTo(Parser.Token.END_ARRAY));
        assertThat(blockAwareJsonParser.getLevel(), equalTo(1));
        blockAwareJsonParser.exitBlock();
        assertThat(parser.currentToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(Parser.Token.END_OBJECT));
        assertThat(parser.nextToken(), nullValue());
    }
}