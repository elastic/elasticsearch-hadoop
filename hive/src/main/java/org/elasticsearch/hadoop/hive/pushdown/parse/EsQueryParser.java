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
package org.elasticsearch.hadoop.hive.pushdown.parse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.BoolJson;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.JsonObj;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.JsonObjManager;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.TermJson;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * parsing es.query value from Hive <tt>TBLPROPERTIES</tt>, then tranfroming to JsonObj.
 * <p>
 * there are three formats of es.query: UrlSearch, DslSearch, PathSearch.
 */
public class EsQueryParser {

    protected static Log log = LogFactory.getLog(EsQueryParser.class);

    protected final Configuration conf;
    protected final boolean isES50;

    public EsQueryParser(Configuration conf, boolean isES50) {
        this.conf = conf;
        this.isES50 = isES50;
    }

    /**
     * parse es.query,
     * <p>
     * then choose and execute the appropriate method according to the es.query props
     *
     * @param rawQuery
     * @return
     */
    public JsonObj parse(String rawQuery) {
        if (rawQuery == null)
            return null;
        rawQuery = rawQuery.trim();

        if (rawQuery.startsWith("{")) {
            //dsl search
            return parseDslQuery(rawQuery);
        } else if (rawQuery.startsWith("?")) {
            //url search
            return parseUriQuery(rawQuery);
        } else {
            //path search
            return parseAsHdfsPath(rawQuery);
        }
    }


    /**
     * parse dsl search, it is equal to json format.
     *
     * @param jsonQuery
     * @return
     */
    protected JsonObj parseDslQuery(String jsonQuery) {
        try {
            return new ObjectMapper().readValue(jsonQuery, JsonObj.class);
        } catch (Exception e) {
            log.error("[parseDslQuery] " + e.getMessage() + ", " + jsonQuery, e);
            return null;
        }
    }


    /**
     * parse url search
     *
     * @param uriQuery
     * @return
     */
    protected JsonObj parseUriQuery(String uriQuery) {
        try {
            //remove ? symbol.
            uriQuery = uriQuery.substring(1);
            String[] kvs = uriQuery.split("&");
            List<JsonObj> filters = new LinkedList<JsonObj>();
            for (String kv : kvs) {
                String[] items = kv.split("=");
                filters.add(new TermJson(items[0], items[1]));
            }

            if (isES50) {
                BoolJson wrapper = new BoolJson();
                wrapper.put("filter", filters);
                return wrapper;
            } else {
                JsonObj and = JsonObjManager.and(isES50, filters);
                return and;
            }
        } catch (Exception e) {
            log.error("[parseUriQuery] " + e.getMessage() + ", " + uriQuery, e);
            return null;
        }
    }

    /**
     * parse path search
     * <p>
     * 1.load the file about query condition from HDFS.
     * <p>
     * 2.parse file content.
     * <p>
     * 3.transform to json call the function of url search or dsl search.
     *
     * @param hdfsPath
     * @return
     */
    protected JsonObj parseAsHdfsPath(String hdfsPath) {

        try {
            String rawQuery = parseAsHdfsPath(hdfsPath, this.conf);

            if (rawQuery == null)
                return null;

            rawQuery = rawQuery.trim();
            if (rawQuery.startsWith("{")) {
                return parseDslQuery(rawQuery);
            } else if (rawQuery.startsWith("?")) {
                return parseUriQuery(rawQuery);
            } else {
                throw new Exception("Invalid query: ");
            }
        } catch (Exception e) {
            log.error("[parseAsHdfsPath] " + e.getMessage() + ", " + hdfsPath, e);
            return null;
        }
    }

    /**
     * load hdfs path and return file content
     *
     * @param hdfsPath
     * @param conf
     * @return
     * @throws IOException
     */
    private String parseAsHdfsPath(String hdfsPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        InputStream in = null;
        try {
            //1.open hdfs file stream
            //2.transform stream to string.
            in = fs.open(new Path(hdfsPath));
            return IOUtils.toString(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

}
