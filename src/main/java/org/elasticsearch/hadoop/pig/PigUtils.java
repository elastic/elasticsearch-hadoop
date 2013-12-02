package org.elasticsearch.hadoop.pig;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;


class PigUtils {

    private static final String MAPPING_NAMES = "es.mapping.names";

    private static boolean pig11Available;

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
            throw new IllegalArgumentException(String.format("Cannot convert [%s] to date", pigDate));
        }

        static Object convertFromES(String esDate) {
            return DatatypeConverter.parseDateTime(esDate).getTimeInMillis();
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
    }

    static FieldAlias load(Settings settings) {
        List<String> aliases = StringUtils.tokenize(settings.getProperty(MAPPING_NAMES), ",");

        Map<String, String> aliasMap = new LinkedHashMap<String, String>();

        if (aliases != null) {
            for (String string : aliases) {
                // split alias
                string = string.trim();
                int index = string.indexOf(":");
                if (index > 0) {
                    String key = string.substring(0, index);
                    // save the lower case version as well to speed, lookup
                    aliasMap.put(key, string.substring(index + 1));
                    aliasMap.put(key.toLowerCase(), string.substring(index + 1));
                }
            }
        }

        return new FieldAlias(aliasMap);
    }

    static String asProjection(Schema schema, Properties props) {
        List<String> fields = new ArrayList<String>();
        addField(schema, fields, load(new PropertiesSettings(props)), "");

        return StringUtils.concatenate(fields.toArray(new String[fields.size()]), ",");
    }

    private static void addField(Schema schema, List<String> fields, FieldAlias fa, String currentNode) {
        for (FieldSchema field : schema.getFields()) {
            if (field.schema != null) {
                addField(schema, fields, fa, currentNode + "." + fa.toES(field.alias));
            }
            else {
                fields.add(fa.toES(field.alias));
                //                if (!StringUtils.hasText(field.alias)) {
                //                    LogFactory.getLog(PigUtils.class).debug("Cannot detect alias for field in schema" + schema);
                //                    return null;
                //                }
            }
        }
    }

    static String asProjection(RequiredFieldList list, Properties props) {
        List<String> fields = new ArrayList<String>();
        for (RequiredField field : list.getFields()) {
            addField(field, fields, load(new PropertiesSettings(props)), "");
        }

        return StringUtils.concatenate(fields.toArray(new String[fields.size()]), ",");
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
}