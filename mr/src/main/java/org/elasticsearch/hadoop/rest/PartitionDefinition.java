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

import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.elasticsearch.hadoop.util.StringUtils.EMPTY_ARRAY;

/**
 * Represents a logical split of an elasticsearch query.
 */
public class PartitionDefinition implements Serializable, Comparable<PartitionDefinition> {
    private final String index;
    private final int shardId;
    private final Slice slice;
    private final String serializedSettings, serializedMapping;
    private final String[] locations;

    public PartitionDefinition(Settings settings, Mapping mapping, String index, int shardId) {
        this(settings, mapping, index, shardId, null, EMPTY_ARRAY);
    }

    public PartitionDefinition(Settings settings, Mapping mapping, String index, int shardId, String[] locations) {
        this(settings, mapping, index, shardId, null, locations);
    }

    public PartitionDefinition(Settings settings, Mapping mapping, String index, int shardId, Slice slice) {
        this(settings, mapping, index, shardId, slice, EMPTY_ARRAY);
    }

    /**
     *
     * @param settings The settings for the partition reader
     * @param mapping The mapping of the index
     * @param index The index name the partition will be executed on
     * @param shardId The shard id the partition will be executed on
     * @param slice The slice the partition will be executed on or null
     * @param locations The locations where to find nodes (hostname:port or ip:port) that can execute the partition locally
     */
    public PartitionDefinition(Settings settings, Mapping mapping, String index, int shardId, Slice slice, String[] locations) {
        this.index = index;
        this.shardId = shardId;
        if (settings != null) {
            this.serializedSettings = settings.save();
        } else {
            this.serializedSettings = null;
        }
        if (mapping != null) {
            this.serializedMapping = IOUtils.serializeToBase64(mapping);
        } else {
            this.serializedMapping = null;
        }
        this.slice = slice;
        this.locations = locations;
    }

    public PartitionDefinition(DataInput in) throws IOException {
        this.index = in.readUTF();
        this.shardId = in.readInt();
        if (in.readBoolean()) {
            this.slice = new Slice(in.readInt(), in.readInt());
        } else {
            this.slice = null;
        }

        if (in.readBoolean()) {
            int length = in.readInt();
            byte[] utf = new byte[length];
            in.readFully(utf);
            this.serializedSettings = StringUtils.asUTFString(utf);
        } else {
            this.serializedSettings = null;
        }
        if (in.readBoolean()) {
            int length = in.readInt();
            byte[] utf = new byte[length];
            in.readFully(utf);
            this.serializedMapping = StringUtils.asUTFString(utf);
        } else {
            this.serializedMapping = null;
        }

        int length = in.readInt();
        locations = new String[length];
        for (int i = 0; i < length; i++) {
            locations[i] = in.readUTF();
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeInt(shardId);
        out.writeBoolean(slice != null);
        if (slice != null) {
            out.writeInt(slice.id);
            out.writeInt(slice.max);
        }

        out.writeBoolean(serializedSettings != null);
        if (serializedSettings != null) {
            // same goes for settings
            byte[] utf = StringUtils.toUTF(serializedSettings);
            out.writeInt(utf.length);
            out.write(utf);
        }
        out.writeBoolean(serializedMapping != null);
        if (serializedMapping != null) {
            // avoid using writeUTF since the mapping can be longer than 65K
            byte[] utf = StringUtils.toUTF(serializedMapping);
            out.writeInt(utf.length);
            out.write(utf);
        }

        out.writeInt(locations.length);
        for (String location : locations) {
            out.writeUTF(location);
        }
    }

    public String getIndex() {
        return index;
    }

    public int getShardId() {
        return shardId;
    }

    public Slice getSlice() {
        return slice;
    }

    public String getSerializedSettings() {
        return serializedSettings;
    }

    public String getSerializedMapping() {
        return serializedMapping;
    }

    public String[] getLocations() {
        return locations;
    }

    public String[] getHostNames() {
        String[] newLocations = new String[locations.length];
        for (int i = 0; i < locations.length; i++) {
            newLocations[i] = StringUtils.parseIpAddress(locations[i]).ip;
        }
        return newLocations;
    }

    public Settings settings() {
        PropertiesSettings settings = new PropertiesSettings();
        return serializedSettings != null ? settings.load(serializedSettings) : settings;
    }

    @Override
    public int compareTo(PartitionDefinition o) {
        int cmp = index.compareTo(o.index);
        if (cmp != 0) {
            return cmp;
        }
        cmp = shardId - o.shardId;
        if (cmp != 0) {
            return cmp;
        }
        if (slice != null) {
            return slice.compareTo(o.slice);
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionDefinition that = (PartitionDefinition) o;

        if (shardId != that.shardId) return false;
        if (!index.equals(that.index)) return false;
        return slice != null ? slice.equals(that.slice) : that.slice == null;

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + shardId;
        result = 31 * result + (slice != null ? slice.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PartitionDefinition{" +
                "index=" + index +
                ", shardId=" + shardId +
                (slice != null ? ", slice=" + slice.id + "/" + slice.max : "") +
                ", locations=" + Arrays.toString(locations) +
                '}';
    }

    public static class Slice implements Serializable, Comparable<Slice> {
        public final int id;
        public final int max;

        public Slice(int id, int max) {
            this.id = id;
            this.max = max;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Slice slice = (Slice) o;

            if (id != slice.id) return false;
            return max == slice.max;

        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + max;
            return result;
        }

        @Override
        public int compareTo(Slice o) {
            int cmp = id - o.id;
            if (cmp != 0) {
                return cmp;
            }
            return max - o.max;
        }
    }
}
