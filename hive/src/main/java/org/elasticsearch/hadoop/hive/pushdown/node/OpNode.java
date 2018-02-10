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
package org.elasticsearch.hadoop.hive.pushdown.node;

import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.elasticsearch.hadoop.hive.pushdown.SargableParser;

/**
 * Internal node is an opNode, while an opNode may not be a internal node.
 */
public class OpNode extends Node {


    protected String operator;

    /**
     * whether current operator neef scan all table or not.
     */
    protected boolean scanAllTable = true;

    /**
     * whether current node, include all descendant nodes, is all optimizable or not.
     */
    protected boolean isAllOptimizable = false;

    /**
     * whether is a and / or logical operator or not
     */
    protected boolean isLogicOp = false;

    public OpNode(String expression) {
        super(expression);
    }

    public OpNode() {
    }

    public boolean isRootOp() {
        if (getOperator() == null) return false;
        return ROOT_NAME.equals(getOperator());
    }

    @Override
    public ExprNodeGenericFuncDesc getExprNode() {
        return (ExprNodeGenericFuncDesc) rawHiveExprNode;
    }

    public boolean checkIsAllOptimizable(SargableParser sargableParser) {
        if (operator == null || operator.isEmpty())
            return isAllOptimizable = true;

        boolean scan = true;
        for (Node n : getChildren()) {
            if (n instanceof OpNode) {
                scan &= ((OpNode) n).isAllOptimizable();
            }
        }
        return isAllOptimizable = scan;
    }

    /**
     * by checking its son node to determine whether the current node expression sweep table
     *
     * @return
     */
    public boolean checkNeedScanAllTable(SargableParser sargableParser) {
        if (operator == null || operator.isEmpty())
            return scanAllTable = true;

        if ("and".equals(operator.toLowerCase())) {
            boolean scan = true;
            for (Node n : getChildren()) {
                if (n instanceof OpNode) {
                    scan &= ((OpNode) n).isScanAllTable();
                }
            }
            return scanAllTable = scan;
        } else if (sargableParser.isSargableOp(getOperator())) {
            boolean scan = true;
            boolean hasOpChildren = false;
            for (Node n : getChildren()) {
                if (n instanceof OpNode) {
                    hasOpChildren = true;
                    scan &= ((OpNode) n).isScanAllTable();
                }
            }
            if (!hasOpChildren) scan = false;
            return scanAllTable = scan;
        } else if (sargableParser.isLogicOp(getOperator()) || ROOT_NAME.equals(getOperator())) {
            boolean scan = false;
            for (Node n : getChildren()) {
                if (n instanceof OpNode) {
                    scan |= ((OpNode) n).isScanAllTable();
                }
            }
            return scanAllTable = scan;
        } else
            return scanAllTable = true;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public boolean isScanAllTable() {
        return scanAllTable;
    }

    public void setScanAllTable(boolean scanAllTable) {
        this.scanAllTable = scanAllTable;
    }

    public boolean isAllOptimizable() {
        return isAllOptimizable;
    }

    public void setAllOptimizable(boolean allOptimizable) {
        isAllOptimizable = allOptimizable;
    }

    public boolean isLogicOp() {
        return isLogicOp;
    }

    public void setLogicOp(boolean logicOp) {
        isLogicOp = logicOp;
    }

    @Override
    public String toString() {
        return "OpNode{" + super.expression +
                ", operator='" + operator + '\'' +
                ", scanAllTable=" + scanAllTable +
                ", isLogicOp=" + isLogicOp +
                '}';
    }

    public static OpNode createRootNode() {
        OpNode opNode = new OpNode();
        opNode.setOperator(ROOT_NAME);
        opNode.setLogicOp(false);
        return opNode;
    }
}
