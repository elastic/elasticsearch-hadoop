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
package org.elasticsearch.hadoop.pushdown;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.elasticsearch.hadoop.hive.pushdown.EsSargableParser;
import org.elasticsearch.hadoop.hive.pushdown.HiveTreeBuilder;
import org.elasticsearch.hadoop.hive.pushdown.node.OpNode;
import org.elasticsearch.hadoop.hive.pushdown.parse.EsTreeParser;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.JsonObj;
import org.elasticsearch.hadoop.util.Assert;
import org.junit.Test;


public class ParseTest {

    @Test
    public void testExprNodeToEsQuery() {
        //ExprNodeDescSeri is the value after serialization of 'type = 1 and publish_date > 1514736000'.
        String exprNodeDescSeri = "AQEAamF2YS51dGlsLkFycmF5TGlz9AECAQFvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLnBsYW4uRXhwck5vZGVHZW5lcmljRnVuY0Rlc+MBAQABAgECb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5wbGFuLkV4cHJOb2RlQ29sdW1uRGVz4wEBdHlw5QAAAWVzX3Rlc3RfswEDb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5zZXJkZTIudHlwZWluZm8uUHJpbWl0aXZlVHlwZUluZu8BAXN0cmlu5wEEb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5wbGFuLkV4cHJOb2RlQ29uc3RhbnREZXPjAQEDCQMBgjEBBW9yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwudWRmLmdlbmVyaWMuR2VuZXJpY1VERk9QRXF1YewBAAABgj0BRVFVQcwBBm9yZy5hcGFjaGUuaGFkb29wLmlvLkJvb2xlYW5Xcml0YWJs5QEAAAEDAQFib29sZWHuAQEBAQABAgECAQFwdWJsaXNoX2RhdOUAAAgBAwEBaW70AQQBAQMXAoCmyKQLAQdvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLnVkZi5nZW5lcmljLkdlbmVyaWNVREZPUEdyZWF0ZXJUaGHuAQAAAYI+AUdSRUFURVIgVEhBzgEGAQAAAQMRAQhvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLnVkZi5nZW5lcmljLkdlbmVyaWNVREZPUEFu5AEBBgEAAAEDEQ==";
        ExprNodeGenericFuncDesc exprNodeDesc = Utilities.deserializeExpression(exprNodeDescSeri);

        EsSargableParser sargableParser = EsSargableParser.getInstance();
        HiveTreeBuilder hiveTreeBuilder = new HiveTreeBuilder(null, sargableParser);
        EsTreeParser parser = new EsTreeParser(sargableParser, true);

        //1.parse expr node desc, and create nodes about the tree structure.
        OpNode root = hiveTreeBuilder.build(exprNodeDesc);
        //2.transform the data of nodes to es query
        JsonObj esQuery = parser.parse(root);
        System.out.println(esQuery);
        Assert.notNull(esQuery);

    }


}
