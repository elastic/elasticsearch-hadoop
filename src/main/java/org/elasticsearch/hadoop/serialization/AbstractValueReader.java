/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.serialization;

import org.elasticsearch.hadoop.serialization.Parser.Token;

/**
 * Base value reader handling the common parsing tasks.
 */
public abstract class AbstractValueReader implements ValueReader {

    private CharSequence failedToken;

    @Override
    public Object read(Parser parser) {
        Token token = parser.nextToken();

        switch (token) {
        case START_OBJECT:
            return readList(parser);
        case START_ARRAY:
            return readArray(parser);
        case FIELD_NAME:
            return readField(parser.currentName());
        default: {
            // error - recall token and move on
            failedToken = parser.currentName();
            return null;
        }
        }
    }

    protected abstract Object readField(String currentName);

    protected abstract Object readArray(Parser parser);

    protected abstract Object readList(Parser parser);

    @Override
    public CharSequence failedToken() {
        return failedToken;
    }
}
