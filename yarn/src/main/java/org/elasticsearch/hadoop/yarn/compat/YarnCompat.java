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
package org.elasticsearch.hadoop.yarn.compat;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.elasticsearch.hadoop.yarn.util.ReflectionUtils;
import org.elasticsearch.hadoop.yarn.util.YarnUtils;

/**
 * Series of compatibility utils around YARN classes...
 */
public abstract class YarnCompat {

	private static Method SET_VIRTUAL_CORES;

	// set virtual cores is not available on all Hadoop 2.x distros
	public static void setVirtualCores(Configuration cfg, Resource resource, int vCores) {
		if (SET_VIRTUAL_CORES == null) {
			SET_VIRTUAL_CORES = ReflectionUtils.findMethod(Resource.class, "setVirtualCores", int.class);
		}
		if (SET_VIRTUAL_CORES != null) {
			ReflectionUtils.invoke(SET_VIRTUAL_CORES, resource, YarnUtils.minVCores(cfg, vCores));
		}
	}

	public static Resource resource(Configuration cfg, int memory, int cores) {
		Resource resource = Records.newRecord(Resource.class);
		resource.setMemory(YarnUtils.minMemory(cfg, memory));
		setVirtualCores(cfg, resource, cores);
		return resource;
	}
}
