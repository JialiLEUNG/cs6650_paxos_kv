//package com.github.jiali.paxos.kvTest;
//
//import com.github.jiali.paxos.main.KvPaxosServer;
//import com.github.jiali.paxos.nodes.PaxosCallbackImpl;
//
//import java.io.IOException;
//
//public class Server5 {
//    public static void main(String[] args) {
//        try {
//            KvPaxosServer server = new KvPaxosServer("./serverConfiguration/server5conf.json");
//            server.setGroupId(1, new PaxosCallbackImpl());
//            server.setGroupId(2, new PaxosCallbackImpl());
//            server.start();
//        } catch (IOException | InterruptedException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//}
