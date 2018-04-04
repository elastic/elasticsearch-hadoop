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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.hive.pushdown.Pair;
import org.elasticsearch.hadoop.hive.pushdown.node.Node;
import org.elasticsearch.hadoop.hive.pushdown.node.OpNode;

import java.util.ArrayList;
import java.util.List;

/**
 * PredicateHandler is a abstract class and it provides a basic implement framework
 * for pushdown optimization.
 */
public abstract class PredicateHandler<T> implements HiveStoragePredicateHandler {

    protected static final Log log = LogFactory.getLog(PredicateHandler.class);

    public abstract Pair<Node, T> optimizePushdown(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc);

    public ExprNodeGenericFuncDesc translate2ExprNodeGenericFuncDesc(ExprNodeDesc desc) {
        if (desc instanceof ExprNodeGenericFuncDesc)
            return (ExprNodeGenericFuncDesc) desc;
        else
            return null;
    }

    public String serializePPD(T obj) {
        if (obj != null)
            return obj.toString();
        else
            return null;
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
        if (exprNodeDesc == null)
            return null;
        Pair<Node, T> pair = this.optimizePushdown(jobConf, deserializer, exprNodeDesc);

        OpNode root = (OpNode) pair.getFirst();

        if (root == null || root.getChildren() == null || root.getChildren().isEmpty() || root.isScanAllTable())
            return null;

        ExprNodeGenericFuncDesc residual = null;
        if (!root.isAllOptimizable()) {
            residual = findResidual(root);
        }

        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = translate2ExprNodeGenericFuncDesc(exprNodeDesc);
        decomposedPredicate.residualPredicate = residual;
        decomposedPredicate.pushedPredicateObject = serializePPD(pair.getSecond());

        if (decomposedPredicate.pushedPredicate == null) {
            return null;
        } else
            return decomposedPredicate;
    }

    /**
     * recursively traverses all nodes
     *
     * @param node
     * @return
     */
    public ExprNodeGenericFuncDesc findResidual(OpNode node) {
        if (node.getExprNode().getGenericUDF().getClass().equals(GenericUDFOPAnd.class)) {
            List<ExprNodeDesc> childrenExprs = new ArrayList<ExprNodeDesc>();
            List<Node> childrenNodes = new ArrayList<Node>();

            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                for (Node child : node.getChildren()) {
                    if (child instanceof OpNode) {
                        if (!((OpNode) child).isAllOptimizable()) {
                            childrenExprs.add(child.getExprNode());
                            childrenNodes.add(child);
                        }
                    }
                }
            }

            if (childrenExprs.isEmpty()) {
                return null;
            } else if (childrenExprs.size() == 2) {
                return node.getExprNode();
            } else {
                return findResidual((OpNode) childrenNodes.get(0));
            }
        } else
            return node.getExprNode();
    }
}
