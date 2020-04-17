package com.github.jiali.paxos.main;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

import com.github.jiali.paxos.nodes.NodeType;
import com.github.jiali.paxos.utils.client.PaxosClient;
import com.github.jiali.paxos.utils.client.PaxosClientImpl;
import com.github.jiali.paxos.datagram.Datagram;
import com.github.jiali.paxos.datagram.DatagramBun;
import com.github.jiali.paxos.datagram.Value;
import com.github.jiali.paxos.utils.serializable.ObjectSerialize;
import com.github.jiali.paxos.utils.serializable.ObjectSerializeImpl;

public class KvPaxosClient {
	// 要发往的proposer的host地址
	private String host;
	// proposer的port
	private int port;
	// comm client
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
	 * 设置发送缓冲区大小
	 * @param bufferSize
	 */
//	public void setSendBufferSize(int bufferSize) {
//		this.bufferSize = bufferSize;
//	}
	
	/**
	 * 设置对端地址
	 * @param host
	 * @param port
	 */
	public void setRemoteAddress(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	/**
	 * 刷新buffer
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
	 *  提交提案
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
