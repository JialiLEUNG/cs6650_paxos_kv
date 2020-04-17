package com.github.jiali.paxos.utils.server;

public interface CommServer {
	public byte[] recvFrom() throws InterruptedException;
}
