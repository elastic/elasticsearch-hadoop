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
