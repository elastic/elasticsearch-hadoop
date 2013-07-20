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

import java.util.List;
import java.util.ListIterator;

import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

public class ContentConsumer {

    private final Parser parser;
    private final ValueReader reader;

    private ContentConsumer(Parser parser, ValueReader reader) {
        Assert.notNull(parser);
        this.parser = parser;
        this.reader = reader;
    }

    public static ContentConsumer consume(Parser parser, ValueReader reader) {
        return new ContentConsumer(parser, reader);
    }

    public <T> T consume(Class<T> type) {
        return consume(null, type);
    }

    @SuppressWarnings("unchecked")
    public <T> T consume(String path, Class<T> type) {
        seekToken(path, parser);
        return (T) token();
    }

    static void seekToken(String path, Parser parser) {
        // return current token if no path is given
        if (!StringUtils.hasText(path)) {
            return;
        }

        List<String> tokens = StringUtils.tokenize(path, "/");
        ListIterator<String> li = tokens.listIterator();
        doSeekToken(li.next(), li, parser);
    }

    private static boolean doSeekToken(String targetNode, ListIterator<String> listIterator, Parser parser) {
        Token token = null;
        
		while ((token = parser.nextToken()) != null) {
			if (token == Token.FIELD_NAME) {
				// found node, go one level deep
				if (targetNode.equals(parser.currentName())) {
					if (listIterator.hasNext()) {
						return doSeekToken(listIterator.next(), listIterator, parser);
					} else {
						return true;
					}
				}
				else {
					// get field token (can be value, object or array)
					parser.nextToken();
					parser.skipChildren();
				}
			}
		}
        return false;
    }

    public Object token() {
        Object result = reader.read(parser);
        if (result == null) {
            throw new SerializationException(String.format("Cannot parse [%s] using reader [%s]", reader.failedToken(),
                    reader));
        }
        return result;
    }

    public void close() {
        parser.close();
    }
}
