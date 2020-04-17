package com.github.jiali.paxos.utils.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class PaxosClientImpl implements PaxosClient {

	@Override
	public void sendTo(String ip, int port, byte[] msg) throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		Socket socket = new Socket(ip, port);
		socket.getOutputStream().write(msg);
		socket.getOutputStream().close();
		socket.close();
	}
}
