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
package org.elasticsearch.spark.serialization;

import java.io.Serializable;

public class Bean implements Serializable {

	private String foo;
	private Number id;
	private boolean bool;

	public Bean() {}
	
	public Bean(String foo, Number bar, boolean bool) {
		this.foo = foo;
		this.id = bar;
		this.bool = bool;
	}
	public String getFoo() {
		return foo;
	}
	public void setFoo(String foo) {
		this.foo = foo;
	}
	public Number getId() {
		return id;
	}
	public void setBar(Number bar) {
		this.id = bar;
	}
	public boolean isBool() {
		return bool;
	}
}
