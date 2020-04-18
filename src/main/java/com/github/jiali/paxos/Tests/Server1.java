package com.github.jiali.paxos.Tests;

import com.github.jiali.paxos.main.KvPaxosServer;
import com.github.jiali.paxos.nodes.PaxosCallbackImpl;

import java.io.IOException;

public class Server1 {
	public static void main(String[] args) {
		try {
			// /Users/hcloud/Desktop/Jiali/NEU/courses/6650_Distributed_System/projects/sample/MyPaxos/MyPaxos/serverConfiguration/server1conf.json
			KvPaxosServer server = new KvPaxosServer(args[0]);
			server.setGroupId(1, new PaxosCallbackImpl());
			server.setGroupId(2, new PaxosCallbackImpl());
			server.start();
		} catch (IOException | InterruptedException | ClassNotFoundException e) {
//			e.printStackTrace();
		}
	}
}
