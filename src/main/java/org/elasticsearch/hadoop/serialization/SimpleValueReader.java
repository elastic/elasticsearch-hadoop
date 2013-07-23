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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;

/**
 * Basic value reader handling the common parsing tasks.
 */
public class SimpleValueReader implements FieldReader {

	private CharSequence failedToken;

	@Override
	public Object read(Parser parser) {
		Token token = parser.currentToken();

		if (token == null) {
			token = parser.nextToken();
		}

		try {
			return read(token, parser);
		} catch (RuntimeException ex) {
			failedToken = parser.currentName();
			throw ex;
		}
	}

	protected Object list(Parser parser) {
		Token t = parser.currentToken();

		if (t == null) {
			t = parser.nextToken();
		}
		if (t == Token.START_ARRAY) {
			t = parser.nextToken();
		}

		List<Object> list = createList();
		for (; t != Token.END_ARRAY; t = parser.nextToken()) {
			list.add(read(t, parser));
		}
		return list;
	}
	
	protected List<Object> createList() {
		return new ArrayList<Object>();
	}

	@SuppressWarnings("unchecked")
	protected Object map(Parser parser) {
		Token t = parser.currentToken();

		if (t == null) {
			t = parser.nextToken();
		}
		if (t == Token.START_OBJECT) {
			t = parser.nextToken();
		}

		Map map = createMap();
		for (; t == Token.FIELD_NAME; t = parser.nextToken()) {
			// Must point to field name
			Object fieldName = fieldName(parser.currentName());
			// And then the value...
			t = parser.nextToken();
			map.put(fieldName, read(t, parser));
		}
		return map;
	}

	protected Object fieldName(String name) {
		return name;
	}

	protected Map<?, ?> createMap() {
		return new LinkedHashMap<Object, Object>();
	}

	protected Object read(Token t, Parser parser) {
		if (t == Token.VALUE_NULL) {
			return nullValue(parser);
		} else if (t == Token.VALUE_STRING) {
			return textValue(parser);
		} else if (t == Token.VALUE_NUMBER) {
			NumberType numberType = parser.numberType();
			if (numberType == NumberType.INT) {
				return intValue(parser);
			} else if (numberType == NumberType.LONG) {
				return longValue(parser);
			} else if (numberType == NumberType.FLOAT) {
				return floatValue(parser);
			} else if (numberType == NumberType.DOUBLE) {
				return doubleValue(parser);
			}
		} else if (t == Token.VALUE_BOOLEAN) {
			return booleanValue(parser);
		} else if (t == Token.START_OBJECT) {
			return map(parser);
		} else if (t == Token.START_ARRAY) {
			return list(parser);
		} else if (t == Token.VALUE_EMBEDDED_OBJECT) {
			return binaryValue(parser);
		}
		return null;
	}

	protected Object binaryValue(Parser parser) {
		return parser.binaryValue();
	}

	protected Object booleanValue(Parser parser) {
		return parser.booleanValue();
	}

	protected Object doubleValue(Parser parser) {
		return parser.doubleValue();
	}

	protected Object floatValue(Parser parser) {
		return parser.floatValue();
	}

	protected Object longValue(Parser parser) {
		return parser.longValue();
	}

	protected Object intValue(Parser parser) {
		return parser.intValue();
	}

	protected Object textValue(Parser parser) {
		return parser.text();
	}

	protected Object nullValue(Parser parser) {
		return null;
	}

	@Override
	public CharSequence failedToken() {
		return failedToken;
	}
}