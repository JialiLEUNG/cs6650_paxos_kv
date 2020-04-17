package com.github.jiali.paxos.datagram;

import java.io.Serializable;

public class DatagramBun implements Serializable {
//	private static final long serialVersionUID = 6483171509762611173L;
	private String type;
	private Object payload;
	public DatagramBun(String type, Object payload) {
		super();
		this.type = type;
		this.payload = payload;
	}
	public String getType() {
		return type;
	}
	public Object getPayload() {
		return payload;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setPayload(Object payload) {
		this.payload = payload;
	}
}
