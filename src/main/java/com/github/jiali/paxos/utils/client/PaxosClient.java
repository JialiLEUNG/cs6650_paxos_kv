package com.github.jiali.paxos.utils.client;

import java.io.IOException;
import java.net.UnknownHostException;

public interface PaxosClient {
	public void sendTo(String ip, int port, byte[] msg) throws UnknownHostException, IOException;
}
