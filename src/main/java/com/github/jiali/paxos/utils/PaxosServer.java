package com.github.jiali.paxos.utils;

public interface PaxosServer {
	public byte[] recvFrom() throws InterruptedException;
}
