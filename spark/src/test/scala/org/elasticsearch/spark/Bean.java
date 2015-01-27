package org.elasticsearch.spark;

import java.io.Serializable;

public class Bean implements Serializable {

	private String foo;
	private Number bar;
	private boolean bool;

	public Bean() {}
	
	public Bean(String foo, Number bar, boolean bool) {
		this.foo = foo;
		this.bar = bar;
		this.bool = bool;
	}
	public String getFoo() {
		return foo;
	}
	public void setFoo(String foo) {
		this.foo = foo;
	}
	public Number getBar() {
		return bar;
	}
	public void setBar(Number bar) {
		this.bar = bar;
	}
	public boolean isBool() {
		return bool;
	}
}
