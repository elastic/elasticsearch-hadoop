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

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * abstract base node
 */
public abstract class Node implements Serializable {

    /**
     * ROOT NODE
     */
    public static final String ROOT_NAME = "ROOT";

    protected ExprNodeDesc rawHiveExprNode = null;

    protected String expression;

    // transient in case of cycle ref
    protected transient List<Node> children = new LinkedList<Node>();

    public Node(String expression) {
        this.expression = expression;
    }

    public Node() {
    }

    public void setExprNode(ExprNodeDesc rawHiveExprNode) {
        this.rawHiveExprNode = rawHiveExprNode;
    }

    public ExprNodeDesc getExprNode() {
        return rawHiveExprNode;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }

    public Node addChild(Node wn) {
        this.children.add(wn);
        return this;
    }

    public boolean hasChildren() {
        return !this.children.isEmpty();
    }

    @Override
    public String toString() {
        return "expression=" + expression;
    }

}
