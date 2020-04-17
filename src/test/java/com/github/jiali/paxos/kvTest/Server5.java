package com.github.jiali.paxos.kvTest;

import com.github.jiali.paxos.main.KvPaxos;

import java.io.IOException;

public class Server5 {
    public static void main(String[] args) {
        try {
            KvPaxos server = new KvPaxos("./serverConfiguration/server5conf.json");
            server.setGroupId(1, new KvCallback());
            server.setGroupId(2, new KvCallback());
            server.start();
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
