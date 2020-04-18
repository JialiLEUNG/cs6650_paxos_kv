package com.github.jiali.paxos.nodes;

public interface PaxosCallback {
	/**
	 * to execute operations as requested
	 * @param msg
	 */
	void callback(byte[] msg);
}
