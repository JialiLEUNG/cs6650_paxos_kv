package com.github.jiali.paxos.core;

public interface PaxosCallback {
	/**
	 * 执行器，用于执行确定的状态
	 * @param msg
	 */
	void callback(byte[] msg);
}
