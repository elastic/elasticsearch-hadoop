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

package org.elasticsearch.hadoop.rest;

import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopException;
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
    
    @SuppressWarnings("rawtypes")
	public EsHadoopException extractErrorWithCause(Map m) {
    	Object type = m.get("type");
    	Object reason = m.get("reason");
    	Object causedBy = m.get("caused_by");
    	
    	EsHadoopException ex = null;
    	if(reason != null) {
    		if(type != null) {
    			ex = new EsHadoopRemoteException(type.toString(), reason.toString());
    		} else {
    			ex = new EsHadoopRemoteException(reason.toString());
    		}
    	}
    	if(causedBy != null) {
    		if(ex == null) {
    			ex = extractErrorWithCause((Map)causedBy);
    		} else {
    			ex.initCause(extractErrorWithCause((Map)causedBy));
    		}
    	}
    	
    	if(ex == null) {
    		ex = new EsHadoopRemoteException(m.toString());
    	}
    	
    	return ex;
    }

    @SuppressWarnings("rawtypes")
	public EsHadoopException extractError(Map jsonMap) {
        Object err = jsonMap.get("error");
        EsHadoopException error = null;
        if (err != null) {
            // part of ES 2.0
            if (err instanceof Map) {
                Map m = ((Map) err);
                err = m.get("root_cause");
                if (err == null) {
                    error = extractErrorWithCause(m);
                }
                else {
                    if (err instanceof List) {
                        Object nested = ((List) err).get(0);
                        if (nested instanceof Map) {
                            Map nestedM = (Map) nested;
                            if (nestedM.containsKey("reason")) {
                            	error = extractErrorWithCause(nestedM);
                            }
                            else {
                                error = new EsHadoopRemoteException(nested.toString());
                            }
                        }
                        else {
                        	error = new EsHadoopRemoteException(nested.toString());
                        }
                    }
                    else {
                    	error = new EsHadoopRemoteException(err.toString());
                    }
                }
            }
            else {
            	error = new EsHadoopRemoteException(err.toString());
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
