package com.github.jiali.paxos.utils;

import com.github.jiali.paxos.utils.PaxosClient;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class PaxosClientImpl implements PaxosClient {

	@Override
	public void sendTo(String ip, int port, byte[] msg) throws UnknownHostException, IOException {
		Socket socket = new Socket(ip, port);
		socket.getOutputStream().write(msg);
		socket.getOutputStream().close();
		socket.close();
	}
}
