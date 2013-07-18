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
package org.elasticsearch.hadoop.serialization.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.SerializationException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.hadoop.serialization.Parser;

public class JacksonJsonParser implements Parser {

    private static final JsonFactory JSON_FACTORY;
    private final JsonParser parser;

    static {
        JSON_FACTORY = new JsonFactory();
        JSON_FACTORY.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        JSON_FACTORY.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        JSON_FACTORY.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }

    public JacksonJsonParser(InputStream in) {
        try {
            this.parser = JSON_FACTORY.createJsonParser(in);
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public JacksonJsonParser(byte[] content) {
        try {
            this.parser = JSON_FACTORY.createJsonParser(content);
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public Token currentToken() {
        return convertToken(parser.getCurrentToken());
    }

    @Override
    public Token nextToken() {
        try {
            return convertToken(parser.nextToken());
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    private Token convertToken(JsonToken token) {
        if (token == null) {
            return null;
        }
        switch (token) {
        case FIELD_NAME:
            return Token.FIELD_NAME;
        case VALUE_FALSE:
        case VALUE_TRUE:
            return Token.VALUE_BOOLEAN;
        case VALUE_STRING:
            return Token.VALUE_STRING;
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
            return Token.VALUE_NUMBER;
        case VALUE_NULL:
            return Token.VALUE_NULL;
        case START_OBJECT:
            return Token.START_OBJECT;
        case END_OBJECT:
            return Token.END_OBJECT;
        case START_ARRAY:
            return Token.START_ARRAY;
        case END_ARRAY:
            return Token.END_ARRAY;
        case VALUE_EMBEDDED_OBJECT:
            return Token.VALUE_EMBEDDED_OBJECT;
        }
        throw new SerializationException("No matching token for json_token [" + token + "]");
    }

    @Override
    public void skipChildren() {
        try {
            parser.skipChildren();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public String currentName() {
        try {
            return parser.getCurrentName();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public Map<String, Object> map() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();

        Token t = currentToken();

        if (t == null) {
            t = nextToken();
        }
        if (t == Token.START_OBJECT) {
            t = nextToken();
        }
        for (; t == Token.FIELD_NAME; t = nextToken()) {
            // Must point to field name
            String fieldName = currentName();
            // And then the value...
            t = nextToken();
            Object value = readValue(t);
            map.put(fieldName, value);
        }
        return map;
    }

    private List<Object> list(Token t) {
        List<Object> list = new ArrayList<Object>();
        while ((t = nextToken()) != Token.END_ARRAY) {
            list.add(readValue(t));
        }
        return list;
    }

    private Object readValue(Token t) {
        if (t == Token.VALUE_NULL) {
            return null;
        }
        else if (t == Token.VALUE_STRING) {
            return text();
        }
        else if (t == Token.VALUE_NUMBER) {
            NumberType numberType = numberType();
            if (numberType == NumberType.INT) {
                return intValue();
            }
            else if (numberType == NumberType.LONG) {
                return longValue();
            }
            else if (numberType == NumberType.FLOAT) {
                return floatValue();
            }
            else if (numberType == NumberType.DOUBLE) {
                return doubleValue();
            }
        }
        else if (t == Token.VALUE_BOOLEAN) {
            return booleanValue();
        }
        else if (t == Token.START_OBJECT) {
            return map();
        }
        else if (t == Token.START_ARRAY) {
            return list(t);
        }
        else if (t == Token.VALUE_EMBEDDED_OBJECT) {
            return binaryValue();
        }
        return null;
    }

    @Override
    public String text() {
        try {
            return parser.getText();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public byte[] bytes() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Number numberValue() {
        try {
            return parser.getNumberValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public NumberType numberType() {
        try {
            return convertNumberType(parser.getNumberType());
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public short shortValue() {
        try {
            return parser.getShortValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public int intValue() {
        try {
            return parser.getIntValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public long longValue() {
        try {
            return parser.getLongValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public float floatValue() {
        try {
            return parser.getFloatValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public double doubleValue() {
        try {
            return parser.getDoubleValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public boolean booleanValue() {
        try {
            return parser.getBooleanValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public byte[] binaryValue() {
        try {
            return parser.getBinaryValue();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public void close() {
        try {
            parser.close();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    private NumberType convertNumberType(JsonParser.NumberType numberType) {
        switch (numberType) {
        case INT:
            return NumberType.INT;
        case LONG:
            return NumberType.LONG;
        case FLOAT:
            return NumberType.FLOAT;
        case DOUBLE:
            return NumberType.DOUBLE;
        case BIG_INTEGER:
            return NumberType.DOUBLE;
        case BIG_DECIMAL:
            return NumberType.DOUBLE;
        }
        throw new ElasticSearchIllegalStateException("No matching token for number_type [" + numberType + "]");
    }
}