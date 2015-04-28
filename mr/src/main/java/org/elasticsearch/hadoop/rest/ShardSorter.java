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

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.elasticsearch.hadoop.serialization.dto.Node;
import org.elasticsearch.hadoop.serialization.dto.Shard;
import org.elasticsearch.hadoop.util.Assert;

// Utility introduced for sorting shard overlaps across multiple nodes. Occurs when dealing with aliases that involve searching multiple indices whom shards (primary or replicas)
// might sit on the same node. As the preference API does not allow a shard for a given index to be selected, the shard with the given ID for the entire alias is used instead which
// results in duplicates.
// As a workaround for this, in case of aliases, the search_shard information is retrieved and the possible combinations of nodes are searched for duplicates for all shards. The
// combination with the most nodes is selected.
//
// If no combination is possible, the preferred node option is used instead.

abstract class ShardSorter {

    public static Map<Shard, Node> find(List<List<Map<String, Object>>> targetShards, Map<String, Node> httpNodes, Log log) {
        // group the shards per node
        Map<Node, Set<Shard>> shardsPerNode = new LinkedHashMap<Node, Set<Shard>>();
        // nodes for each shard
        Map<SimpleShard, Set<Node>> nodesForShard = new LinkedHashMap<SimpleShard, Set<Node>>();

        // for each shard group
        for (List<Map<String, Object>> shardGroup : targetShards) {
            for (Map<String, Object> shardData : shardGroup) {
                Shard shard = new Shard(shardData);
                Node node = httpNodes.get(shard.getNode());
                if (node == null) {
                    log.warn(String.format("Cannot find node with id [%s] (is HTTP enabled?) from shard [%s] in nodes [%s]; layout [%s]", shard.getNode(), shard, httpNodes, targetShards));
                    return Collections.emptyMap();
                }

                // node -> shards
                Set<Shard> shardSet = shardsPerNode.get(node);
                if (shardSet == null) {
                    shardSet = new LinkedHashSet<Shard>();
                    shardsPerNode.put(node, shardSet);
                }
                shardSet.add(shard);

                // shard -> nodes
                SimpleShard ss = SimpleShard.from(shard);
                Set<Node> nodeSet = nodesForShard.get(ss);
                if (nodeSet == null) {
                    nodeSet = new LinkedHashSet<Node>();
                    nodesForShard.put(ss, nodeSet);
                }
                nodeSet.add(node);
            }
        }

        return checkCombo(httpNodes.values(), shardsPerNode, targetShards.size());
    }

    private static Map<Shard, Node> checkCombo(Collection<Node> nodes, Map<Node, Set<Shard>> shardsPerNode, int numberOfShards) {
        List<Set<Node>> nodesCombinations = powerList(new LinkedHashSet<Node>(nodes));

        Set<SimpleShard> shards = new LinkedHashSet<SimpleShard>();
        boolean overlappingShards = false;
        // try each combination and check if there are duplicates
        for (Set<Node> set : nodesCombinations) {
            shards.clear();
            overlappingShards = false;

            for (Node node : set) {
                Set<Shard> associatedShards = shardsPerNode.get(node);
                if (associatedShards != null) {
                    for (Shard shard : associatedShards) {
                        if (!shards.add(SimpleShard.from(shard))) {
                            overlappingShards = true;
                            break;
                        }
                    }
                    if (overlappingShards) {
                        break;
                    }
                }
            }
            // bingo!
            if (!overlappingShards && shards.size() == numberOfShards) {
                Map<Shard, Node> finalShards = new LinkedHashMap<Shard, Node>();
                for (Node node : set) {
                    Set<Shard> associatedShards = shardsPerNode.get(node);
                    if (associatedShards != null) {
                        // to avoid shard overlapping, only add one request for each shard # (regardless of its index) per node
                        Set<Integer> shardIds = new HashSet<Integer>();
                        for (Shard potentialShard : associatedShards) {
                            if (shardIds.add(potentialShard.getName())) {
                                finalShards.put(potentialShard, node);
                            }
                        }
                    }
                }

                return finalShards;
            }
        }
        return Collections.emptyMap();
    }

    static class SimpleShard {
        private final String index;
        private final Integer id;

        private SimpleShard(String index, Integer id) {
            this.index = index;
            this.id = id;
        }

        static SimpleShard from(Shard shard) {
            return new SimpleShard(shard.getIndex(), shard.getName());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((index == null) ? 0 : index.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SimpleShard other = (SimpleShard) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            }
            else if (!id.equals(other.id))
                return false;
            if (index == null) {
                if (other.index != null)
                    return false;
            }
            else if (!index.equals(other.index))
                return false;
            return true;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("SimpleShard [index=").append(index).append(", id=").append(id).append("]");
            return builder.toString();
        }
    }

    static <E> List<Set<E>> nodesCombinations(Set<E> set) {
        // remove empty or 1 element set
        List<Set<E>> list = powerList(set);
        for (Iterator<Set<E>> iterator = list.iterator(); iterator.hasNext();) {
            Set<E> s = iterator.next();
            if (s.size() < 2) {
                iterator.remove();
            }
        }
        return list;
    }

    // create the possible combinations using a power set. The results are afterwards sorted based on their set size.
    static <E> List<Set<E>> powerList(Set<E> set) {
        List<Set<E>> list = new ArrayList<Set<E>>(new PowerSet<E>(set));
        Collections.sort(list, new SetLengthComparator<E>());
        return list;
    }

    private static class SetLengthComparator<T> implements Comparator<Set<T>> {
        @Override
        public int compare(Set<T> o1, Set<T> o2) {
            return -Integer.compare(o1.size(), o2.size());
        }
    }

    private static class PowerSet<E> extends AbstractSet<Set<E>> {
        private final Map<E, Integer> input;

        PowerSet(Set<E> set) {
            Assert.isTrue(set.size() <= 30, "Too many elements to create a power set " + set.size());

            input = new LinkedHashMap<E, Integer>(set.size());
            int i = set.size();
            for (E e : set) {
                input.put(e, Integer.valueOf(i--));
            }
        }

        @Override
        public Iterator<Set<E>> iterator() {
            return new ReverseIndexedListIterator<Set<E>>(size()) {
                @Override
                protected Set<E> get(final int setBits) {
                    return new SubSet<E>(input, setBits);
                }
            };
        }

        @Override
        public int size() {
            return 1 << input.size();
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            if (o instanceof Set) {
                Set<?> set = (Set<?>) o;
                return input.keySet().containsAll(set);
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof PowerSet) {
                PowerSet<?> that = (PowerSet<?>) o;
                return input.equals(that.input);
            }
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return input.keySet().hashCode() << (input.size() - 1);
        }
    }

    private static final class SubSet<E> extends AbstractSet<E> {
        private final Map<E, Integer> inputSet;
        private final int mask;

        SubSet(Map<E, Integer> inputSet, int mask) {
            this.inputSet = inputSet;
            this.mask = mask;
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {
                final List<E> elements = new ArrayList<E>(inputSet.keySet());
                int remainingSetBits = mask;

                @Override
                public boolean hasNext() {
                    return remainingSetBits != 0;
                }

                @Override
                public E next() {
                    int index = Integer.numberOfTrailingZeros(remainingSetBits);
                    if (index == 32) {
                        throw new NoSuchElementException();
                    }
                    remainingSetBits &= ~(1 << index);
                    return elements.get(index);
                }

                @Override
                public final void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int size() {
            return Integer.bitCount(mask);
        }

        @Override
        public boolean contains(Object o) {
            Integer index = inputSet.get(o);
            return index != null && (mask & (1 << index)) != 0;
        }
    }

    private static abstract class ReverseIndexedListIterator<E> implements Iterator<E> {
        private final int size;
        private int position;

        protected ReverseIndexedListIterator(int size) {
            this.size = size;
            this.position = size - 1;
        }

        @Override
        public final boolean hasNext() {
            return position > 0;
        }

        @Override
        public final E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return get(position--);
        }

        public final int nextIndex() {
            return position;
        }

        public final boolean hasPrevious() {
            return position < size;
        }

        public final E previous() {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }
            return get(++position);
        }

        public final int previousIndex() {
            return position + 1;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }

        protected abstract E get(int index);
    }
}