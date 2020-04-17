/**
 * This project is inspired by https://github.com/luohaha/MyPaxos
 */
package com.github.jiali.paxos.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


import com.github.jiali.paxos.core.Proposer;
import com.github.jiali.paxos.utils.FileUtils;
import com.github.jiali.paxos.utils.client.PaxosClient;
import com.github.jiali.paxos.utils.client.PaxosClientImpl;
import com.github.jiali.paxos.core.Accepter;
import com.github.jiali.paxos.core.NodeConfiguration;
import com.github.jiali.paxos.core.NodeInfo;
import com.github.jiali.paxos.core.Learner;
import com.github.jiali.paxos.core.PaxosCallback;
import com.github.jiali.paxos.packet.Packet;
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

	private Map<Integer, Accepter> groupidToAccepter = new HashMap<>();

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
		this.nodeConfiguration = gson.fromJson(FileUtils.readFromFile(this.confFile), NodeConfiguration.class);
		this.nodeInfo = getMyAcceptOrProposer(this.nodeConfiguration.getNodes());
		// 启动客户端
		this.client = new PaxosClientImpl();
		//this.logger.setLevel(Level.WARNING);
	}

	/**
	 * 设置log级别
	 * @param level
	 * 级别
	 */
	public void setLogLevel(Level level) {
		this.logger.setLevel(level);
	}

	/**
	 * 
	 * @param
	 * @param executor
	 */
	public void setGroupId(int groupId, PaxosCallback executor) {
		Accepter accepter = new Accepter(nodeInfo.getId(), nodeConfiguration.getNodes(), nodeInfo, nodeConfiguration, groupId,
				this.client);
		Proposer proposer = new Proposer(nodeInfo.getId(), nodeConfiguration.getNodes(), nodeInfo, nodeConfiguration.getTimeout(),
				accepter, groupId, this.client);
		Learner learner = new Learner(nodeInfo.getId(), nodeConfiguration.getNodes(), nodeInfo, nodeConfiguration, accepter,
				executor, groupId, this.client);
		this.groupidToCallback.put(groupId, executor);
		this.groupidToAccepter.put(groupId, accepter);
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
		System.out.println("Server " + nodeConfiguration.getMyid() + " starts!");
		while (true) {
			byte[] data = server.recvFrom();
			//Packet packet = gson.fromJson(new String(data), Packet.class);
			Packet packet = objectSerialize.byteArrayToObject(data, Packet.class);
			int groupId = packet.getGroupId();
			Accepter accepter = this.groupidToAccepter.get(groupId);
			Proposer proposer = this.groupidToProposer.get(groupId);
			Learner learner = this.groupidToLearner.get(groupId);
			if (accepter == null || proposer == null || learner == null) {
				return;
			}
			switch (packet.getNodeType()) {
			case ACCEPTER:
				accepter.sendPacket(packet.getPacketBean());
				break;
			case PROPOSER:
				proposer.sendPacket(packet.getPacketBean());
				break;
			case LEARNER:
				learner.sendPacket(packet.getPacketBean());
				break;
			case SERVER:
				proposer.sendPacket(packet.getPacketBean());
				break;
			default:
				break;
			}
		}
	}
}
