package com.github.jiali.paxos.main;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

import com.github.jiali.paxos.nodes.NodeType;
import com.github.jiali.paxos.utils.PaxosClient;
import com.github.jiali.paxos.utils.PaxosClientImpl;
import com.github.jiali.paxos.datagram.Datagram;
import com.github.jiali.paxos.datagram.DatagramBun;
import com.github.jiali.paxos.datagram.Value;
import com.github.jiali.paxos.utils.ObjectSerialize;
import com.github.jiali.paxos.utils.ObjectSerializeImpl;

public class KvPaxosClient {
	// which propose to connect to (by IP address)
	private String host;
	// proposer which is connected by the client (by port)
	private int port;
	// paxos client
	private PaxosClient paxosClient;
	// buffer size
	private int bufferSize = 8;
	// buffer
	private Queue<byte[]> buffer; 

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();
	
	public KvPaxosClient() throws IOException {
		super();
		this.paxosClient = new PaxosClientImpl();
		this.buffer = new ArrayDeque<>();
	}

	/**
	 * 设置对端地址
	 * @param host
	 * @param port
	 */
	public void targetAddress(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	/**
	 * flush buffer
	 * @param groupId
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void flush(int groupId) throws UnknownHostException, IOException {
		if (this.buffer.isEmpty())
			return;
		UUID uuid = UUID.randomUUID();
		Datagram datagram = new Datagram(new DatagramBun("SubmitPacket", new Value(uuid, this.objectSerialize.objectToObjectArray(this.buffer))), groupId, NodeType.SERVER);
		this.paxosClient.sendTo(this.host, this.port, this.objectSerialize.objectToObjectArray(datagram));
		this.buffer.clear();
	}
	
	/**
	 *  submit
	 * @param value
	 * @param groupId
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void submit(byte[] value, int groupId) throws Exception, UnknownHostException, IOException {
		if (this.host == null)
			throw new Exception();
		this.buffer.add(value);
		if (this.buffer.size() >= this.bufferSize) {
			flush(groupId);
		}
	}
}
