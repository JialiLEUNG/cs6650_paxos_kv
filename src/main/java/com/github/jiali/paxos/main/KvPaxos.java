/**
 * This project is inspired by https://github.com/luohaha/MyPaxos
 */
package com.github.jiali.paxos.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.github.jiali.paxos.nodes.Proposer;
import com.github.jiali.paxos.utils.FilesHandler;
import com.github.jiali.paxos.utils.client.PaxosClient;
import com.github.jiali.paxos.utils.client.PaxosClientImpl;
import com.github.jiali.paxos.nodes.Acceptor;
import com.github.jiali.paxos.nodes.NodeConfiguration;
import com.github.jiali.paxos.nodes.NodeInfo;
import com.github.jiali.paxos.nodes.Learner;
import com.github.jiali.paxos.nodes.PaxosCallback;
import com.github.jiali.paxos.datagram.Datagram;
import com.github.jiali.paxos.utils.serializable.ObjectSerialize;
import com.github.jiali.paxos.utils.serializable.ObjectSerializeImpl;
import com.github.jiali.paxos.utils.server.CommServer;
import com.github.jiali.paxos.utils.server.CommServerImpl;
import com.google.gson.Gson;

public class KvPaxos {

	/**
	 * 全局配置文件信息
	 */
	private NodeConfiguration nodeConfiguration;

	/**
	 * 本节点的信息
	 */
	private NodeInfo nodeInfo;

	/**
	 * 配置文件所在的位置
	 */
	private String confFile;

	private Map<Integer, PaxosCallback> groupidToCallback = new HashMap<>();

	private Map<Integer, Proposer> groupidToProposer = new HashMap<>();

	private Map<Integer, Acceptor> groupidToAccepter = new HashMap<>();

	private Map<Integer, Learner> groupidToLearner = new HashMap<>();

	private Gson gson = new Gson();
	
	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();

	private Logger logger = Logger.getLogger("KV-Paxos");

	/*
	 * 客户端
	 */
	private PaxosClient client;

	public KvPaxos(String confFile) throws IOException {
		super();
		this.confFile = confFile;
		this.nodeConfiguration = gson.fromJson(FilesHandler.readFromFile(this.confFile), NodeConfiguration.class);
		this.nodeInfo = getMyAcceptOrProposer(this.nodeConfiguration.getNodes());
		// client
		this.client = new PaxosClientImpl();
	}


	/**
	 *
	 * @param
	 * @param executor
	 */
	public void setGroupId(int groupId, PaxosCallback executor) {
		Acceptor acceptor = new Acceptor(nodeInfo.getId(), nodeConfiguration.getNodes(), nodeInfo, nodeConfiguration, groupId,
				this.client);
		Proposer proposer = new Proposer(nodeInfo.getId(), nodeConfiguration.getNodes(), nodeInfo, nodeConfiguration.getTimeout(),
				acceptor, groupId, this.client);
		Learner learner = new Learner(nodeInfo.getId(), nodeConfiguration.getNodes(), nodeInfo, nodeConfiguration, acceptor,
				executor, groupId, this.client);
		this.groupidToCallback.put(groupId, executor);
		this.groupidToAccepter.put(groupId, acceptor);
		this.groupidToProposer.put(groupId, proposer);
		this.groupidToLearner.put(groupId, learner);
	}

	/**
	 * 获得我的accepter或者proposer信息
	 * 
	 * @param
	 * @return
	 */
	private NodeInfo getMyAcceptOrProposer(List<NodeInfo> nodeInfos) {
		for (NodeInfo each : nodeInfos) {
			if (each.getId() == nodeConfiguration.getMyid()) {
				return each;
			}
		}
		return null;
	}

	/**
	 * 启动paxos服务器
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException 
	 */
	public void start() throws IOException, InterruptedException, ClassNotFoundException {
		// 启动paxos服务器
		CommServer server = new CommServerImpl(this.nodeInfo.getPort());
		System.out.println("Server " + nodeConfiguration.getMyid() + " connection starts.");
		while (true) {
			byte[] data = server.recvFrom();
			Datagram Datagram = objectSerialize.byteArrayToObject(data, Datagram.class);
			int groupId = Datagram.getGroupId();
			Proposer proposer = this.groupidToProposer.get(groupId);
			Acceptor acceptor = this.groupidToAccepter.get(groupId);
			Learner learner = this.groupidToLearner.get(groupId);
			if (proposer == null || acceptor == null ||  learner == null) {
				return;
			}
			switch (Datagram.getNodeType()) {
			case ACCEPTER:
				acceptor.sendDatagram(Datagram.getDatagramBun());
				break;
			case PROPOSER:
				proposer.sendPacket(Datagram.getDatagramBun());
				break;
			case LEARNER:
				learner.sendPacket(Datagram.getDatagramBun());
				break;
			case SERVER:
				proposer.sendPacket(Datagram.getDatagramBun());
				break;
			default:
				break;
			}
		}
	}
}
