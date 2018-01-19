package org.elasticsearch.hadoop.rest;

import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.EsMajorVersion;

/**
 * Encapsulates logic for parsing and understanding error messages from Elasticsearch.
 */
public class ErrorExtractor {

    private final EsMajorVersion internalVersion;

    public ErrorExtractor(EsMajorVersion internalVersion) {
        this.internalVersion = internalVersion;
    }

    public String extractError(Map jsonMap) {
        Object err = jsonMap.get("error");
        String error = "";
        if (err != null) {
            // part of ES 2.0
            if (err instanceof Map) {
                Map m = ((Map) err);
                err = m.get("root_cause");
                if (err == null) {
                    if (m.containsKey("reason")) {
                        error = m.get("reason").toString();
                    }
                    else if (m.containsKey("caused_by")) {
                        error += ";" + ((Map) m.get("caused_by")).get("reason");
                    }
                    else {
                        error = m.toString();
                    }
                }
                else {
                    if (err instanceof List) {
                        Object nested = ((List) err).get(0);
                        if (nested instanceof Map) {
                            Map nestedM = (Map) nested;
                            if (nestedM.containsKey("reason")) {
                                error = nestedM.get("reason").toString();
                            }
                            else {
                                error = nested.toString();
                            }
                        }
                        else {
                            error = nested.toString();
                        }
                    }
                    else {
                        error = err.toString();
                    }
                }
            }
            else {
                error = err.toString();
            }
        }
        return error;
    }

    public String prettify(String error) {
        if (internalVersion.onOrAfter(EsMajorVersion.V_2_X)) {
            return error;
        }

        String invalidFragment = ErrorUtils.extractInvalidXContent(error);
        String header = (invalidFragment != null ? "Invalid JSON fragment received[" + invalidFragment + "]" : "");
        return header + "[" + error + "]";
    }

    public String prettify(String error, ByteSequence body) {
        if (internalVersion.onOrAfter(EsMajorVersion.V_2_X)) {
            return error;
        }
        String message = ErrorUtils.extractJsonParse(error, body);
        return (message != null ? error + "; fragment[" + message + "]" : error);
    }
}
