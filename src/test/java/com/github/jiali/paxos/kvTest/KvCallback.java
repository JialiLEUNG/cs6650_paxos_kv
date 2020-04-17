package com.github.jiali.paxos.kvTest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.github.jiali.paxos.nodes.PaxosCallback;
import com.google.gson.Gson;

public class KvCallback implements PaxosCallback {
	/**
	 * hashmap to store kvStore pair.
	 */
	private ConcurrentMap<String, String> kvStore = new ConcurrentHashMap<>();
	private Gson gson = new Gson();

	@Override
	public void callback(byte[] msg) {
		/**
		 * three operations: get, put, and delete
		 */
		String msgString = new String(msg);
		// convert JSON string to Java objects
		ClientInput bean = gson.fromJson(msgString, ClientInput.class);
		switch (bean.getOperation()) {
		case "get":
			System.out.println("GET KEY: " + bean.getKey() + "Result: " + kvStore.get(bean.getKey()) + " Succeed");
			break;
		case "put":
			kvStore.put(bean.getKey(), bean.getValue());
			System.out.println("PUT KEY: " + bean.getKey() + " VALUE: " + bean.getValue() + " Succeed.");
			break;
		case "delete":
			kvStore.remove(bean.getKey());
			System.out.println("DELETE KEY: " + bean.getKey() + " Succeed.");
			break;
		default:
			break;
		}
	}
}
