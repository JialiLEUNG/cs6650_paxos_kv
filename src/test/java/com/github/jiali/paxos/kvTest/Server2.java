package com.github.jiali.paxos.kvTest;

import java.io.IOException;

import com.github.jiali.paxos.main.KvPaxos;
import com.github.jiali.paxos.nodes.PaxosCallbackImpl;

public class Server2 {
	public static void main(String[] args) {
		try {
			KvPaxos server = new KvPaxos("./serverConfiguration/server2conf.json");
			server.setGroupId(1, new PaxosCallbackImpl());
			server.setGroupId(2, new PaxosCallbackImpl());
			server.start();
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
