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
package org.elasticsearch.hadoop.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.pushdown.HiveTreeBuilder;
import org.elasticsearch.hadoop.hive.pushdown.PredicateHandler;
import org.elasticsearch.hadoop.hive.pushdown.SargableParser;
import org.elasticsearch.hadoop.hive.pushdown.Utils;
import org.elasticsearch.hadoop.hive.pushdown.function.Pair;
import org.elasticsearch.hadoop.hive.pushdown.node.Node;
import org.elasticsearch.hadoop.hive.pushdown.node.OpNode;
import org.elasticsearch.hadoop.hive.pushdown.parse.EsQueryParser;
import org.elasticsearch.hadoop.hive.pushdown.parse.EsTreeParser;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.JsonObj;
import org.elasticsearch.hadoop.util.SettingsUtils;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY;


/**
 * EsStoragePredicateHandler is an entrance class,for implementing pushdown optimization.
 */
public class EsStoragePredicateHandler extends PredicateHandler<JsonObj> {

    private static Log log = LogFactory.getLog(EsStoragePredicateHandler.class);

    @Override
    public Pair<Node, JsonObj> optimizePushdown(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
        if (exprNodeDesc == null) {
            return new Pair<Node, JsonObj>(null, null);
        }

        // get settings
        Settings settings = HadoopSettingsManager.loadFrom(jobConf);
        boolean isES50 = SettingsUtils.isEs50(settings);
        log.info("[PushDown][isES50] : " + isES50);

        SargableParser sargableParser = Utils.getSargableParser(settings);
        if (sargableParser == null) {
            return new Pair<Node, JsonObj>(null, null);
        }

        //create a hive tree builder for converting ExprNodeDesc to OpNode.
        HiveTreeBuilder hiveTreeBuilder = new HiveTreeBuilder(sargableParser);

        //create a es tree parser for converting OpNode to JsonObj.
        EsTreeParser parser = new EsTreeParser(sargableParser, isES50);

        // if exists es.query prop, then it is a necessary condition to add to the pushdown optimization plan
        if (jobConf != null && jobConf.get(ES_QUERY) != null) {
            String rawQuery = jobConf.get(ES_QUERY);
            log.info("[PushDown][Raw " + ES_QUERY + "] : " + rawQuery);
            JsonObj preFilterJson = new EsQueryParser(jobConf, isES50).parse(rawQuery);
            //add a pre filter condition.
            parser.setPreFilterJson(preFilterJson);
        }


        String esMappingNameProps = jobConf.get(HiveConstants.MAPPING_NAMES);
        if (StringUtils.isNotEmpty(esMappingNameProps)) {
            //set es mapping name
            Map<String, String> mappingNames = new HashMap<String, String>();

            String[] split = esMappingNameProps.split(",");
            for (String value : split) {
                if (StringUtils.isEmpty(value)) continue;
                String[] hiveToEsField = value.split(":");
                if (hiveToEsField.length == 2) {
                    mappingNames.put(hiveToEsField[0].trim(), hiveToEsField[1].trim());
                }
            }

            if (mappingNames.size() > 0) {
                hiveTreeBuilder.setMappingNames(mappingNames);
            }
        }

        OpNode root = hiveTreeBuilder.build(exprNodeDesc);

        if (root != null) {
            JsonObj esQuery = parser.parse(root);
            if (esQuery != null) {
                log.info("[PushDown][Final " + ES_QUERY + "] : " + esQuery.toQuery());
            }
            return new Pair<Node, JsonObj>(root, esQuery);
        }

        return new Pair<Node, JsonObj>(null, null);
    }

}
