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
import org.elasticsearch.hadoop.hive.pushdown.node.Node;
import org.elasticsearch.hadoop.hive.pushdown.node.OpNode;

import java.io.Serializable;
import java.util.List;

/**
 * Parse The structure of the tree parse ,and get a new type about T.
 *
 * @param <T>
 */
public abstract class TreeParser<T> implements Serializable {

    protected static Log log = LogFactory.getLog(TreeParser.class);

    protected SargableParser sargableParser;

    public TreeParser(SargableParser sargableParser) {
        this.sargableParser = sargableParser;
    }

    /**
     * main entry
     *
     * @param root
     * @return
     */
    public T parse(OpNode root) {
        if (root == null || root.isScanAllTable())
            return null;

        return _parse(root);
    }

    protected T _parse(OpNode opNode) {
        if (opNode == null) return null;
        String op = opNode.getOperator();
        if (opNode.isRootOp()) {
            return parseRootNode(opNode);
        } else if (sargableParser.isLogicOp(op)) {
            return parseLogicOp(opNode);
        } else if (sargableParser.isSargableOp(op)) {
            return parseSargableOp(opNode);
        } else
            return null;
    }

    /**
     * parse from root
     *
     * @param root
     * @return
     */
    protected T parseRootNode(OpNode root) {
        Node wn = safeget(root.getChildren(), 0);
        if (wn != null && wn instanceof OpNode) {
            return _parse((OpNode) wn);
        } else
            return null;
    }

    /**
     * parse operators like and/or/not
     *
     * @param logicOp
     * @return
     */
    protected abstract T parseLogicOp(OpNode logicOp);

    /**
     * parse other operators like =,>,<,is_null
     *
     * @param op
     * @return
     */
    protected abstract T parseSargableOp(OpNode op);

    public <T> T safeget(List<T> list, int index) {
        if (list != null && list.size() > index)
            try {
                return list.get(index);
            } catch (Exception e) {
                return null;
            }
        else
            return null;
    }
}
