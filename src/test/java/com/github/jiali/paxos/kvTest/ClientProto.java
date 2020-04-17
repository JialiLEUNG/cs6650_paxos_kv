//package com.github.jiali.paxos.kvTest;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//
//import com.google.protobuf.ByteString;
//import io.grpc.*;
//import java.util.Scanner;
//import java.util.concurrent.TimeUnit;
//
//
//public class Client {
//
//    private String name;
//    private final int deadLineMs = 5000;
//
//    public Client(String name){
//        this.name = name;
//    }
//
//    public String getName(){
//        return name;
//    }
//
//
//    public void run(){
//        // 1. ManagedChannelBuilder
//        // 2. Use Plaintext
//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9090).usePlaintext(true).build();
//
//        try {
//            // The withDeadlineAfter() sets the deadline to 100ms from when the client RPC is set to when the response is picked up by the client.
//            keyValueStore.KeyValueStore. clientStub = keyValueStore.KeyValueStore.newBlockingStub(channel).withDeadlineAfter(deadLineMs, TimeUnit.MILLISECONDS);
//
//            Scanner clientScanner = new Scanner(new File("./src/main/java/ClientRequest.txt"));
//
//            while(clientScanner.hasNext()){
//                String[] requestArr = clientScanner.nextLine().trim().split(" ");
//
//                String msg;
//                if (requestArr.length < 2){
//                    msg = "----- Error: At least 2 argument needed at Time: " + System.currentTimeMillis() +
//                            ". Syntax: <operation> <key> OR <operation> <key> <value>. For example: get apple";
//                    System.out.println(msg);
//                } else{
//                    String action = requestArr[0].toLowerCase();
//                    ByteString key = ByteString.copyFromUtf8(requestArr[1]);
//                    switch(action){
//                        case "get":
//                            doGet(clientStub, key);
//                            break;
//                        case "delete":
//                            doDelete(clientStub, key);
//                            break;
//                        case "put":
//                            if (requestArr.length == 3){
//                                doPut(clientStub, key, ByteString.copyFromUtf8(requestArr[2]));
//                            }
//                            break;
//                        default:
//                            System.err.println("----- Error: Unknown operation at time " + System.currentTimeMillis());
//                            break;
//                    }
//                }
//            }
//        } catch(FileNotFoundException e){
//            System.err.println(e.getMessage());
//        }
//    }
//
//
//
//    // subs -- generated from proto file
//
//    private static void doPut(keyValueStore.PaxosKV.KeyValStoreBlockingStub stub, ByteString key, ByteString value){
//        KeyValueStore.PutResponse res = stub.put(
//                KeyValueStore.PutRequest.newBuilder().setKey(key).setValue(value).build());
//        System.out.println(res.getMsg().toStringUtf8());
//    }
//
//    private static void doGet(keyValueStore.PaxosKV.KeyValStoreBlockingStub stub, ByteString key){
//
//        try{
//            KeyValueStore.GetResponse res = stub.get(
//                    KeyValueStore.GetRequest.newBuilder().setKey(key).build());
//            System.out.println(res.getMsg().toStringUtf8());
//
//            String val = res.getValue().toStringUtf8();
//            if (!val.equals("NULL")){
//                System.out.println("Result of get " + key.toStringUtf8() + ": " + val);
//            }
//        } catch (StatusRuntimeException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static void doDelete(keyValueStore.PaxosKV.KeyValStoreBlockingStub stub, ByteString key){
//        try{
//            KeyValueStore.DeleteResponse res = stub.delete(
//                    KeyValueStore.DeleteRequest.newBuilder().setKey(key).build());
//            System.out.println(res.getMsg().toStringUtf8());
//        } catch (StatusRuntimeException e) {
//            e.printStackTrace();
//        }
//    }
//}
