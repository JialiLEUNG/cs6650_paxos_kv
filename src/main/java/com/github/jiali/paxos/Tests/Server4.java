package com.github.jiali.paxos.Tests;

import com.github.jiali.paxos.main.KvPaxosServer;
import com.github.jiali.paxos.nodes.PaxosCallbackImpl;

import java.io.IOException;

public class Server4 {
    public static void main(String[] args) {
        try {
            KvPaxosServer server = new KvPaxosServer("./serverConfiguration/server4conf.json"); // "./serverConfiguration/server4conf.json"
            server.setGroupId(1, new PaxosCallbackImpl());
            server.setGroupId(2, new PaxosCallbackImpl());
            server.start();
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
//            e.printStackTrace();
        }
    }
}
