package com.github.jiali.paxos.kvTest;

import java.util.HashMap;
import java.util.Map;

import com.github.jiali.paxos.core.PaxosCallback;
import com.google.gson.Gson;

public class KvCallback implements PaxosCallback {
	/**
	 * hashmap to store kv pair.
	 */
	private Map<String, String> kv = new HashMap<>();
	private Gson gson = new Gson();

	@Override
	public void callback(byte[] msg) {
		/**
		 * three operations: get, put, and delete
		 */
		String msString = new String(msg);
		// convert JSON string to Java objects
		ClientInput bean = gson.fromJson(msString, ClientInput.class);
		switch (bean.getOperation()) {
		case "get":
			System.out.println("GET KEY: " + bean.getKey() + "Result: " + kv.get(bean.getKey()) + " Succeed");
			break;
		case "put":
			kv.put(bean.getKey(), bean.getValue());
			System.out.println("PUT KEY: " + bean.getKey() + " VALUE: " + bean.getValue() + " Succeed.");
			break;
		case "delete":
			kv.remove(bean.getKey());
			System.out.println("DELETE KEY: " + bean.getKey() + " Succeed.");
			break;
		default:
			break;
		}
	}
}
