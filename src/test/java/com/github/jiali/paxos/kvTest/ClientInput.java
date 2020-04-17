package com.github.jiali.paxos.kvTest;

public class ClientInput {
	
	private String operation;
	private String key;
	private String value;
	public ClientInput(String operation, String key, String value) {
		super();
		this.operation = operation;
		this.key = key;
		this.value = value;
	}
	public String getOperation() {
		return operation;
	}
	public String getKey() {
		return key;
	}
	public String getValue() {
		return value;
	}
}
