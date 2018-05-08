package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.EsHadoopException;

public class EsHadoopRemoteException extends EsHadoopException {
	private static final long serialVersionUID = 5402297229024034583L;
	
	private String type=null;
	
	public EsHadoopRemoteException(String message) {
		super(message);
	}
	public EsHadoopRemoteException(String message, Throwable throwable) {
		super(message, throwable);
	}
	public EsHadoopRemoteException(String type, String message) {
		super(message);
		this.type = type;
	}
	public EsHadoopRemoteException(String type, String message, Throwable throwable) {
		super(message, throwable);
		this.type = type;
	}
	
	public String getType() {
		return type;
	}
	
	public String toString() {
        String s = getClass().getName();
        String message = getLocalizedMessage();
        String type = this.getType();
        
        final StringBuilder b = new StringBuilder();
        b.append(s);
        if(type != null) {
        	b.append(": ").append(type);
        }
        if(message != null) {
        	b.append(": ").append(message);
        }
        return b.toString();
    }
}
