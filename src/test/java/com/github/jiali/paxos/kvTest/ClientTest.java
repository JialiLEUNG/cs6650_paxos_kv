package com.github.jiali.paxos.kvTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Scanner;

import com.github.jiali.paxos.main.KvPaxosClient;
import com.google.gson.Gson;

public class ClientTest {

	public static void main(String[] args) {
		try {
			KvPaxosClient client = new KvPaxosClient();
//			client.setSendBufferSize(20);
			client.setRemoteAddress("localhost", 3001);
			Gson gson = new Gson();

			Scanner clientScanner = new Scanner(new File("./src/main/java/com/github/jiali/paxos/clientRequests/ClientRequest.txt"));

			while (clientScanner.hasNext()) {
				String[] requestArr = clientScanner.nextLine().trim().split(" ");

				String msg;
				if (requestArr.length < 2) {
					msg = "----- Error: At least 2 argument needed at Time: " + System.currentTimeMillis() +
							". Syntax: <operation> <key> OR <operation> <key> <value>. For example: get apple";
					System.out.println(msg);
				} else {
					String action = requestArr[0].toLowerCase();
					String key = requestArr[1];
					switch (action) {
						case "get":
							client.submit(gson.toJson(new ClientInput("put", key, "")).getBytes(), 1);
							client.flush(1);
							break;
						case "delete":
							client.submit(gson.toJson(new ClientInput("delete", key, "")).getBytes(), 1);
							client.flush(1);
							break;
						case "put":
							if (requestArr.length == 3) {
								client.submit(gson.toJson(new ClientInput("put", key, requestArr[2])).getBytes(), 1);
								client.flush(1);
							}
							break;
						default:
							System.err.println("----- Error: Unknown operation at time " + System.currentTimeMillis());
							break;
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}