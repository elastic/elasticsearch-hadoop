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
package org.elasticsearch.hadoop.hive.pushdown;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.elasticsearch.hadoop.hive.pushdown.node.ConstantNode;
import org.elasticsearch.hadoop.hive.pushdown.node.FieldNode;
import org.elasticsearch.hadoop.hive.pushdown.node.Node;
import org.elasticsearch.hadoop.hive.pushdown.node.OpNode;
import org.elasticsearch.hadoop.util.FieldAlias;

import java.util.ArrayList;
import java.util.List;

/**
 * Build parse from Hive {@link ExprNodeDesc} to a tree
 */
public class HiveTreeBuilder {

    protected static Log log = LogFactory.getLog(HiveTreeBuilder.class);

    protected SargableParser sargableParser = null;

    /**
     * load es.mapping,names ,then get the field mmaping of hive to elasticsearch
     */
    protected FieldAlias fieldAlias = null;

    public HiveTreeBuilder(FieldAlias fieldAlias, SargableParser sargableParser) {
        this.sargableParser = sargableParser;
        this.fieldAlias = fieldAlias;
    }

    /**
     * get real elasticsearch field name.
     *
     * @param field
     * @return when mapping contain filed, return the mapping value. otherwise return itself.
     */
    private String getMappingField(String field) {
        if (fieldAlias == null) return field;
        String mappingField = fieldAlias.toES(field);
        if (mappingField == null)
            return field;
        else
            return mappingField;
    }

    public OpNode build(ExprNodeDesc exprNodeDesc) {
        OpNode root = OpNode.createRootNode();
        _build(root, exprNodeDesc);
        root.checkNeedScanAllTable(sargableParser);
        root.checkIsAllOptimizable(sargableParser);

        return root;
    }

    private void _build(Node nowParent, ExprNodeDesc hiveNode) {
        if (hiveNode.getChildren() == null || hiveNode.getChildren().isEmpty()) {
            if (hiveNode.getName().endsWith("ExprNodeColumnDesc") && !hiveNode.getCols().isEmpty()) {
                FieldNode node = new FieldNode(hiveNode.getExprString());
                node.setFieldType(hiveNode.getTypeString());
                String field = hiveNode.getCols().get(0);
                node.setField(getMappingField(field));
                node.setExprNode(hiveNode);
                nowParent.addChild(node);
            } else if (hiveNode.getName().endsWith("ExprNodeConstantDesc")) {
                ConstantNode node = new ConstantNode(hiveNode.getExprString());
                node.setValue(hiveNode.getExprString());
                node.setValueType(hiveNode.getTypeString());
                node.setExprNode(hiveNode);
                nowParent.addChild(node);
            } else {
                // other operations
                OpNode opNode = new OpNode(hiveNode.getExprString());
                opNode.setScanAllTable(true);
                opNode.setLogicOp(false);
                opNode.setExprNode(hiveNode);
                nowParent.addChild(opNode);
            }
        } else {
            OpNode opNode = new OpNode(hiveNode.getExprString());
            opNode.setExprNode(hiveNode);

            List<String> childrenExprs = new ArrayList<String>();
            for (ExprNodeDesc nodeDesc : hiveNode.getChildren()) {
                childrenExprs.add(nodeDesc.getExprString());
            }

            String operator = findOp(hiveNode);

            if (operator != null) {
                opNode.setOperator(operator);
                if (sargableParser.isLogicOp(operator)) {
                    opNode.setLogicOp(true);
                }
            }
            nowParent.addChild(opNode);

            if (!sargableParser.isLogicOp(operator) && !sargableParser.isSargableOp(operator)) {
                return;
            }

            for (ExprNodeDesc nodeDesc : hiveNode.getChildren()) {
                _build(opNode, nodeDesc);
            }

            // finally, check whether is current node need scan all data from the target table or not.
            boolean scanAllTable = opNode.checkNeedScanAllTable(sargableParser);
            opNode.checkIsAllOptimizable(sargableParser);
            if (!scanAllTable && sargableParser.reverseOp(operator) != null) {
                if (opNode.getChildren().size() == 2 && opNode.getChildren().get(0) instanceof ConstantNode) {
                    Node swap = opNode.getChildren().get(0);
                    opNode.getChildren().set(0, opNode.getChildren().get(1));
                    opNode.getChildren().set(1, swap);
                    opNode.setOperator(sargableParser.reverseOp(opNode.getOperator()));
                }
            }
        }
    }

    /**
     * @param exprNodeDesc
     * @return
     */
    protected String findOp(ExprNodeDesc exprNodeDesc) {
        String udfName = getGenericUDFNameFromExprDesc(exprNodeDesc);
        if (udfName == null)
            return null;

        String[] arr = udfName.split("\\.");
        udfName = arr[arr.length - 1];
        String op = sargableParser.udfOp(udfName);
        if (op == null)
            return udfName;
        else
            return sargableParser.synonymOp(op);
    }

    public String getGenericUDFNameFromExprDesc(ExprNodeDesc desc) {
        if (!(desc instanceof ExprNodeGenericFuncDesc)) {
            return null;
        } else {
            ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc) desc;
            return genericFuncDesc.getGenericUDF().getUdfName();
        }
    }
}
