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

import org.elasticsearch.hadoop.serialization.Parser;

/**
 * Parser implementation that keeps track of the nested levels within a delegate JSON token stream.
 *
 * Encountering a START_OBJECT or START_ARRAY token increases the nesting level by 1, and encountering
 * an END_OBJECT or END_ARRAY decreases the nesting level by 1. At any time that the parser is nested
 * within an object, it can be directed to skip all blocks until it reaches the same nesting level as
 * when it was created.
 */
public class BlockAwareJsonParser implements Parser {

    private final Parser delegate;
    private int open = 0;

    public BlockAwareJsonParser(Parser delegate) {
        this.delegate = delegate;
        Token currentToken = delegate.currentToken();
        if (currentToken != null && (currentToken == Token.START_OBJECT || currentToken == Token.START_ARRAY)) {
            this.open = 1; // start assuming that we are nested if the current parser token is a block start.
        }
    }

    public Parser getParser() {
        return delegate;
    }

    public int getLevel() {
        return open;
    }

    /**
     * If this parser is reading tokens from an object or an array that is nested below its original
     * nesting level, it will consume and skip all tokens until it reaches the end of the block that
     * it was created on. The underlying parser will be left on the END_X token for the block.
     */
    public void exitBlock() {
        if (open == 0) {
            return;
        }

        if (open < 0) {
            throw new IllegalStateException("Parser is no longer nested in any blocks at the level in which it was " +
                    "created. You must create a new block aware parser to track the levels above this one.");
        }

        while (open > 0) {
            Token t = delegate.nextToken();
            if (t == null) {
                // handle EOF?
                return;
            }
            updateLevelBasedOn(t);
        }
    }

    private void updateLevelBasedOn(Token token) {
        if (token != null) {
            switch (token) {
                case START_OBJECT:
                case START_ARRAY:
                    ++open;
                    break;
                case END_OBJECT:
                case END_ARRAY:
                    --open;
                    break;
            }
        }
    }

    @Override
    public Token currentToken() {
        return delegate.currentToken();
    }

    @Override
    public Token nextToken() {
        Token token = delegate.nextToken();
        updateLevelBasedOn(token);
        return token;
    }

    @Override
    public void skipChildren() {
        delegate.skipChildren();
        updateLevelBasedOn(delegate.currentToken());
    }

    @Override
    public String absoluteName() {
        return delegate.absoluteName();
    }

    @Override
    public String currentName() {
        return delegate.currentName();
    }

    @Override
    public Object currentValue() {
        return delegate.currentValue();
    }

    @Override
    public String text() {
        return delegate.text();
    }

    @Override
    public byte[] bytes() {
        return delegate.bytes();
    }

    @Override
    public Number numberValue() {
        return delegate.numberValue();
    }

    @Override
    public NumberType numberType() {
        return delegate.numberType();
    }

    @Override
    public short shortValue() {
        return delegate.shortValue();
    }

    @Override
    public int intValue() {
        return delegate.intValue();
    }

    @Override
    public long longValue() {
        return delegate.longValue();
    }

    @Override
    public float floatValue() {
        return delegate.floatValue();
    }

    @Override
    public double doubleValue() {
        return delegate.doubleValue();
    }

    @Override
    public boolean booleanValue() {
        return delegate.booleanValue();
    }

    @Override
    public byte[] binaryValue() {
        return delegate.binaryValue();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public int tokenCharOffset() {
        return delegate.tokenCharOffset();
    }
}
