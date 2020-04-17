package com.github.jiali.paxos.kvTest;

import com.github.jiali.paxos.main.KvPaxos;
import com.github.jiali.paxos.nodes.PaxosCallbackImpl;

import java.io.IOException;

public class Server4 {
    public static void main(String[] args) {
        try {
            KvPaxos server = new KvPaxos("./serverConfiguration/server4conf.json");
            server.setGroupId(1, new PaxosCallbackImpl());
            server.setGroupId(2, new PaxosCallbackImpl());
            server.start();
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
