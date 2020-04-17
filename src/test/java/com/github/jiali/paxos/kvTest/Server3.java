package com.github.jiali.paxos.kvTest;

import java.io.IOException;

import com.github.jiali.paxos.main.KvPaxos;

public class Server3 {
	public static void main(String[] args) {
		try {
			KvPaxos server = new KvPaxos("./serverConfiguration/server3conf.json");
			server.setGroupId(1, new KvCallback());
			server.setGroupId(2, new KvCallback());
			server.start();
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
