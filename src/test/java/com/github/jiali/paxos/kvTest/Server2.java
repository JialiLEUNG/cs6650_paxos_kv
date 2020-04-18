//package com.github.jiali.paxos.kvTest;
//
//import java.io.IOException;
//
//import com.github.jiali.paxos.main.KvPaxosServer;
//import com.github.jiali.paxos.nodes.PaxosCallbackImpl;
//
//public class Server2 {
//	public static void main(String[] args) {
//		try {
//			KvPaxosServer server = new KvPaxosServer("./serverConfiguration/server2conf.json");
//			server.setGroupId(1, new PaxosCallbackImpl());
//			server.setGroupId(2, new PaxosCallbackImpl());
//			server.start();
//		} catch (IOException | InterruptedException | ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//	}
//}
