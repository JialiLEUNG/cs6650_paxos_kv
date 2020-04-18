/**
 * This project is inspired by https://github.com/luohaha/MyPaxos
 */
package com.github.jiali.paxos.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.jiali.paxos.nodes.Proposer;
import com.github.jiali.paxos.utils.FilesHandler;
import com.github.jiali.paxos.utils.PaxosClient;
import com.github.jiali.paxos.utils.PaxosClientImpl;
import com.github.jiali.paxos.nodes.Acceptor;
import com.github.jiali.paxos.nodes.NodeConfiguration;
import com.github.jiali.paxos.nodes.NodeInfo;
import com.github.jiali.paxos.nodes.Learner;
import com.github.jiali.paxos.nodes.PaxosCallback;
import com.github.jiali.paxos.datagram.Datagram;
import com.github.jiali.paxos.utils.ObjectSerialize;
import com.github.jiali.paxos.utils.ObjectSerializeImpl;
import com.github.jiali.paxos.utils.PaxosServer;
import com.github.jiali.paxos.utils.PaxosServerImpl;
import com.google.gson.Gson;

public class KvPaxosServer {

	/**
	 * node configuration
	 */
	private NodeConfiguration nodeConfiguration;

	/**
	 * info about current node
	 */
	private NodeInfo nodeInfo;

	/**
	 * where is the configuration file
	 */
	private String confFile;

	// nodes or roles by group: groupID --> role
	private Map<Integer, PaxosCallback> callbackByGroup = new HashMap<>();

	private Map<Integer, Proposer> proposerByGroup = new HashMap<>();

	private Map<Integer, Acceptor> accepterByGroup = new HashMap<>();

	private Map<Integer, Learner> learnerByGroup = new HashMap<>();

	// deserialize server configuration from json files
	private Gson deserializeServerConf = new Gson();
	
	private ObjectSerialize serializeObject = new ObjectSerializeImpl();

	// private Logger logger = Logger.getLogger("KV-Paxos");

	/*
	 * client
	 */
	private PaxosClient client;

	public KvPaxosServer(String confFile) throws IOException {
		super();
		this.confFile = confFile;
		this.nodeConfiguration = deserializeServerConf.fromJson(FilesHandler.readFromFile(this.confFile), NodeConfiguration.class);
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
		this.callbackByGroup.put(groupId, executor);
		this.accepterByGroup.put(groupId, acceptor);
		this.proposerByGroup.put(groupId, proposer);
		this.learnerByGroup.put(groupId, learner);
	}

	/**
	 * get node info about My Acceptor Or Proposer
	 * 
	 * @param
	 * @return
	 */
	private NodeInfo getMyAcceptOrProposer(List<NodeInfo> nodeInfos) {
		for (NodeInfo i : nodeInfos) {
			if (i.getId() == nodeConfiguration.getMyid()) {
				return i;
			}
		}
		return null;
	}

	/**
	 * launch the paxos server
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException 
	 */
	public void start() throws IOException, InterruptedException, ClassNotFoundException {
		PaxosServer server = new PaxosServerImpl(this.nodeInfo.getPort());
//		System.out.println("Server " + nodeConfiguration.getMyid() + " connection starts.");
		System.out.println("Server " + nodeConfiguration.getMyid() + " is running at port " + this.nodeInfo.getPort());
		while (true) {
			byte[] data = server.recvFrom();
			Datagram Datagram = serializeObject.byteArrayToObject(data, Datagram.class);
			int groupId = Datagram.getGroupId();
			Proposer proposer = this.proposerByGroup.get(groupId);
			Acceptor acceptor = this.accepterByGroup.get(groupId);
			Learner learner = this.learnerByGroup.get(groupId);
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
				learner.sendDatagram(Datagram.getDatagramBun());
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
