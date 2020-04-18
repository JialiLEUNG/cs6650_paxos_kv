package com.github.jiali.paxos.nodes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.github.jiali.paxos.clientRequests.ClientInput;
import com.google.gson.Gson;

public class PaxosCallbackImpl implements PaxosCallback {
    /**
     * concurrentHashmap to store kvStore pair.
     */
    private ConcurrentMap<String, String> kvStore = new ConcurrentHashMap<>();
    private Gson gson = new Gson();
    ReadWriteLock rwl = new ReadWriteLock();

    @Override
    public void callback(byte[] msg) {
        /**
         * three operations: get, put, and delete
         */
        String msgString = new String(msg);
        // convert JSON string to Java objects
        ClientInput clientInput = gson.fromJson(msgString, ClientInput.class);
        switch (clientInput.getOperation()) {
            case "get":
                get(clientInput);
                break;
            case "put":
                put(clientInput);
                break;
            case "delete":
                delete(clientInput);
                break;
            default:
                break;
        }
    }

    private void get(ClientInput clientInput) {

        try{
            rwl.lockRead();

            String value = kvStore.get(clientInput.getKey());

            if (value != null){
                // we get as many value objects as we need to return to the client
                // (in this case, we select them from the service’s value collection
                // based on whether they’re inside our request getRequest),
                // and write them each in turn to the response observer using its onNext() method.

                System.out.println("GET KEY: " + clientInput.getKey() + "Result: " + value + " Succeed");
            } else{
                System.out.println("----- Fail: Key does not exist. GET fail.");
            }
            rwl.unlockRead();
        } catch (InterruptedException e) {
        }
    }

    private void put(ClientInput clientInput){
        try{
            rwl.lockWrite();

            String result = kvStore.putIfAbsent(clientInput.getKey(), clientInput.getValue());

            if(result == null){
                System.out.println("PUT KEY: " + clientInput.getKey() + " VALUE: " + clientInput.getValue() + " Succeed.");
            } else{
                System.out.println("----- Fail: Key already exists. PUT fail.");
            }
            rwl.unlockWrite();
        } catch (Exception e){
        }
    }

    private void delete(ClientInput clientInput){
        try{
            rwl.lockWrite();

            String result = kvStore.remove(clientInput.getKey());
            if (result != null){
                System.out.println("DELETE KEY: " + clientInput.getKey() + " Succeed.");
            } else{
                System.out.println("----- Fail: Key does not exist. DELETE fail");
            }
            rwl.unlockWrite();
        }catch (Exception e){
        }
    }
}