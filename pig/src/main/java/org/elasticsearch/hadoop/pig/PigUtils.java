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
package org.elasticsearch.hadoop.pig;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.DateUtils;
import org.elasticsearch.hadoop.util.FieldAlias;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;


class PigUtils {

    static final String MAPPING_NAMES = "es.mapping.names";
    static final String NAMED_TUPLE = "es.mapping.pig.tuple.use.field.names";
    static final boolean NAMED_TUPLE_DEFAULT = false;

    private final static boolean pig11Available;


    static {
        // initialize
        pig11Available = "datetime".equals(DataType.findTypeName((byte) 30));
    }

    static String convertDateToES(Object pigDate) {
        return (pig11Available ? Pig11OrHigherConverter.convertToES(pigDate) : PigUpTo10Converter.convertToES(pigDate));
    }

    static Object convertDateFromES(String esDate) {
        return (pig11Available ? Pig11OrHigherConverter.convertFromES(esDate) : PigUpTo10Converter.convertFromES(esDate));
    }

    static Object convertDateFromES(Long esDate) {
        return (pig11Available ? Pig11OrHigherConverter.convertFromES(esDate) : PigUpTo10Converter.convertFromES(esDate));
    }

    private static abstract class PigUpTo10Converter {
        static String convertToES(Object pigDate) {
            if (pigDate instanceof Number) {
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis(((Number) pigDate).longValue());
                return DatatypeConverter.printDateTime(cal);
            }
            if (pigDate instanceof String) {
                return ((String) pigDate);
            }
            throw new EsHadoopIllegalArgumentException(String.format("Cannot convert [%s] to date", pigDate));
        }

        static Object convertFromES(String esDate) {
            return DateUtils.parseDate(esDate).getTimeInMillis();
        }

        static Object convertFromES(Long esDate) {
            return esDate;
        }
    }

    private static abstract class Pig11OrHigherConverter {
        static String convertToES(Object pigDate) {
            DateTime dt = (DateTime) pigDate;
            // ISODateTimeFormat.dateOptionalTimeParser() throws "printing not supported"
            return dt.toString();
        }

        static Object convertFromES(String esDate) {
            return ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(esDate);
        }

        static Object convertFromES(Long esDate) {
            return new DateTime(esDate, DateTimeZone.UTC);
        }
    }

    static FieldAlias alias(Settings settings) {
        return new FieldAlias(SettingsUtils.aliases(settings.getProperty(MAPPING_NAMES), false), false);
    }

    static String asProjection(Schema schema, Properties props) {
        List<String> fields = new ArrayList<String>();
        addField(schema, fields, alias(new PropertiesSettings(props)), null);

        return StringUtils.concatenate(fields, ",");
    }

    private static void addField(Schema schema, List<String> fields, FieldAlias fa, String currentNode) {
        for (FieldSchema field : schema.getFields()) {
            String node;
            if (field.alias != null) {
                // if no field
                node = fa.toES(field.alias);
                node = (currentNode != null ? currentNode + "." + node : node);
            }
            else {
                node = currentNode;
            }
            // && field.type != DataType.TUPLE
            if (field.schema != null) {
                addField(field.schema, fields, fa, node);
            }
            else {
                if (!StringUtils.hasText(node)) {
                    LogFactory.getLog(PigUtils.class).warn("Cannot detect alias for field in schema" + schema);
                }

                if (node != null) {
                    fields.add(fa.toES(node));
                }
            }
        }
    }

    static String asProjection(RequiredFieldList list, Properties props) {
        List<String> fields = new ArrayList<String>();
        FieldAlias alias = alias(new PropertiesSettings(props));
        for (RequiredField field : list.getFields()) {
            addField(field, fields, alias, "");
        }

        return StringUtils.concatenateAndUriEncode(fields, ",");
    }

    private static void addField(RequiredField field, List<String> fields, FieldAlias fa, String currentNode) {
        if (field.getSubFields() != null && !field.getSubFields().isEmpty()) {
            for (RequiredField subField : field.getSubFields()) {
                addField(subField, fields, fa, currentNode + "." + fa.toES(subField.getAlias()));
            }
        }

        else {
            fields.add(fa.toES(field.getAlias()));
        }
    }

    static boolean isComplexType(ResourceFieldSchema fieldSchema) {
        return (fieldSchema != null && fieldSchema.getType() >= 100);
    }
}