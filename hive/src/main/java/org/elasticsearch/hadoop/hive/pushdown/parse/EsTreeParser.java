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

import org.elasticsearch.hadoop.hive.pushdown.Pair;
import org.elasticsearch.hadoop.hive.pushdown.SargableParser;
import org.elasticsearch.hadoop.hive.pushdown.node.ConstantNode;
import org.elasticsearch.hadoop.hive.pushdown.node.FieldNode;
import org.elasticsearch.hadoop.hive.pushdown.node.Node;
import org.elasticsearch.hadoop.hive.pushdown.node.OpNode;
import org.elasticsearch.hadoop.hive.pushdown.parse.query.*;

import java.io.Serializable;
import java.util.*;

/**
 * Parse The structure of the tree parse
 *
 */
public class EsTreeParser implements Serializable {

    private final SargableParser sargableParser;

    private final boolean isES50;

    public EsTreeParser(SargableParser sargableParser, boolean isES50) {
        this.sargableParser = sargableParser;
        this.isES50 = isES50;
    }

    private boolean isStrictType(FieldNode node) {
        return !node.getFieldType().toLowerCase().equals("string");
    }

    public JsonObj parse(OpNode root) {

        JsonObj pushdown = doParse(root);

        if (isES50) {
            BoolJson wrapper = new BoolJson();

            if (pushdown != null && !pushdown.isEmpty()) {
                wrapper.filter(pushdown);
            }

            JsonObj ret = new JsonObj();
            if (!wrapper.isEmpty()) {
                ret.addByKey(wrapper);
            }

            if (!ret.isEmpty()) {
                return ret;
            }
            else {
                return null;
            }
        } else {
            AndJson andJson = new AndJson();

            if (pushdown != null && !pushdown.isEmpty())
                andJson.filters(pushdown);

            JsonObj ret = new JsonObj();
            if (!andJson.isEmpty())
                ret.addByKey(andJson);

            if (!ret.isEmpty()) {
                return ret;
            }
            else {
                return null;
            }
        }
    }

    protected JsonObj doParse(OpNode opNode) {
        if (opNode == null) { return null;}
        String op = opNode.getOperator();
        if (opNode.isRootOp()) {
            return parseRootNode(opNode);
        } else if (sargableParser.isLogicOp(op)) {
            return parseLogicOp(opNode);
        } else if (sargableParser.isSargableOp(op)) {
            return parseSargableOp(opNode);
        } else {
            return null;
        }
    }

    /**
     * parse from root
     *
     * @param root
     * @return
     */
    protected JsonObj parseRootNode(OpNode root) {
        Node wn = safeget(root.getChildren(), 0);
        if (wn != null && wn instanceof OpNode) {
            return doParse((OpNode) wn);
        } else {
            return null;
        }
    }

    protected JsonObj parseLogicOp(OpNode opNode) {
        String op = opNode.getOperator();

        List<JsonObj> filters = new ArrayList<JsonObj>();
        for (Node node : opNode.getChildren()) {
            if (node instanceof OpNode) {
                OpNode n = (OpNode) node;
                JsonObj childRes = doParse(n);
                if (childRes == null) {
                    if ("or".equals(op)) {
                        return null;
                    }
                } else {
                    JsonObjManager.add(filters, childRes);
                }
            }
        }

        if (!filters.isEmpty()) {
            if ("and".equals(op)) {
                return JsonObjManager.and(isES50, filters);
            } else if ("or".equals(op)) {
                return JsonObjManager.or(isES50, filters);
            } else if ("not".equals(op)) {
                return JsonObjManager.not(isES50, filters.get(0));
            }
        }

        return null;
    }

    protected JsonObj parseSargableOp(OpNode opNode) {
        String op = opNode.getOperator();

        if ("=".equals(op)) {
            FieldNode fieldNode = (FieldNode) opNode.getChildren().get(0);
            Pair<String, List<Object>> pair = simpleExtractFieldNVals(opNode);
            String field = pair.getFirst();
            Object val = safeget(pair.getSecond(), 0);
            String value = val.toString();

            if (isAllNotNull(field, val)) {
                if (isStrictType(fieldNode)) {
                    return new TermJson(field, value);
                } else {
                    if (isES50) {
                        return new MatchJson(field, value);
                    } else {
                        return new QueryJson(new MatchJson(field, value));
                    }
                }
            }
        } else if (sargableParser.isRangeOp(op)) {
            RangeJson rangeJson = new RangeJson();
            if ("between".equals(op)) {
                Pair<String, List<Object>> pair = simpleExtractFieldNVals(opNode, "false");
                String field = pair.getFirst();
                boolean not = false;
                Object val1 = safeget(pair.getSecond(), 0);
                Object val2;
                if (val1.toString().equals("true")) {
                    not = true;
                    val1 = safeget(pair.getSecond(), 1);
                    val2 = safeget(pair.getSecond(), 2);
                } else {
                    val2 = safeget(pair.getSecond(), 1);
                }

                if (isAllNotNull(field, val1, val2)) {
                    if (!not) {
                        return rangeJson.between(field, val1, val2);
                    } else {
                        rangeJson.gt(field, val2).lt(field, val1);
                        return rangeJson;
                    }
                }
            } else {
                Pair<String, List<Object>> pair = simpleExtractFieldNVals(opNode);
                String field = pair.getFirst();
                Object val = safeget(pair.getSecond(), 0);
                if (isAllNotNull(field, val)) {
                    return rangeJson.singleRange(op, field, val);
                }
            }
        } else if ("is null".equals(op)) {
            Pair<String, List<Object>> pair = simpleExtractFieldNVals(opNode);
            String field = pair.getFirst();

            if (isAllNotNull(field)) {
                if (isES50) {
                    BoolJson boolJson = new BoolJson();
                    boolJson.mustNot(new ExistsJson(field));
                    return boolJson;
                } else {
                    return new MissingJson(field);
                }
            }
        } else if ("is not null".equals(op)) {
            Pair<String, List<Object>> pair = simpleExtractFieldNVals(opNode);
            String field = pair.getFirst();

            if (isAllNotNull(field)) {
                return new ExistsJson(field);
            }
        }

        return null;
    }

    private Pair<String, List<Object>> simpleExtractFieldNVals(OpNode opNode, Object... ignoreVals) {
        Set<Object> ignore = new HashSet<Object>();
        if (ignoreVals != null && ignoreVals.length > 0)
            ignore.addAll(Arrays.asList(ignoreVals));

        String field = null;
        List<Object> vals = new ArrayList<Object>();
        for (Node node : opNode.getChildren()) {
            if (node instanceof FieldNode) {
                field = ((FieldNode) node).getField();
            } else if (node instanceof ConstantNode) {
                ConstantNode constantNode = (ConstantNode) node;
                Object v = constantNode.getValue();
                if ("string".equals(constantNode.getValueType().toLowerCase()))
                    v = stripStrVal(v.toString());

                if (ignore.contains(v))
                    continue;
                vals.add(v);
            }
        }

        return new Pair<String, List<Object>>(field, vals);
    }

    private boolean isAllNotNull(Object... objs) {
        for (Object o : objs) {
            if (o == null)
                return false;
        }
        return true;
    }

    private String stripStrVal(String val) {
        if (val.startsWith("'") && val.endsWith("'"))
            val = val.substring(1, val.length() - 1);
        return val;
    }

    private <T> T safeget(List<T> list, int index) {
        if (list != null && list.size() > index) {
            try {
                return list.get(index);
            } catch (Exception e) {
                return null;
            }
        }
        else {
            return null;
        }
    }
}
