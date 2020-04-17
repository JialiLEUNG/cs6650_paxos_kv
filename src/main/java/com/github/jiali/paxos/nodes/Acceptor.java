package com.github.jiali.paxos.nodes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.github.jiali.paxos.datagram.*;
import com.github.jiali.paxos.utils.FilesHandler;
import com.github.jiali.paxos.utils.client.PaxosClient;
import com.github.jiali.paxos.datagram.AcceptRequest;
import com.github.jiali.paxos.utils.serializable.ObjectSerialize;
import com.github.jiali.paxos.utils.serializable.ObjectSerializeImpl;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Acceptor {
	static class Instance {
		// current ballot number
		private int ballot;
		// accepted value
		private Value value;
		// accepted value's ballot
		private int acceptedBallot;

		public Instance(int ballot, Value value, int acceptedBallot) {
			super();
			this.ballot = ballot;
			this.value = value;
			this.acceptedBallot = acceptedBallot;
		}

		public void setValue(Value value) {
			this.value = value;
		}
	}

	// accepter's state, contain each instances
	private Map<Integer, Instance> instanceStatus = new HashMap<>();
	// accepted value
	private Map<Integer, Value> acceptedValue = new HashMap<>();
	// accepter's id
	private transient int id;
	// proposers
	private transient List<NodeInfo> proposers;
	// my conf
	private transient NodeInfo my;
	// the last instance id
	private volatile int lastInstanceId = 0;
	// configuration of the node
	private NodeConfiguration nodeConfiguration;
	// groupID
	private int groupId;

	private Gson gson = new Gson();
	
	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();
	
	private Logger logger = Logger.getLogger("KV-Paxos");
	//client
	private PaxosClient client;

	// message queue to save datagrambun
	private BlockingQueue<DatagramBun> msgQueue = new LinkedBlockingQueue<>();

	public Acceptor(int id, List<NodeInfo> proposers, NodeInfo my, NodeConfiguration nodeConfiguration, int groupId, PaxosClient client) {
		this.id = id;
		this.proposers = proposers;
		this.my = my;
		this.nodeConfiguration = nodeConfiguration;
		this.groupId = groupId;
		this.client = client;
		instanceRecover();
		new Thread(() -> {
			while (true) {
				try {
					DatagramBun msg = msgQueue.take();
					receivePacket(msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}
	
	/**
	 * append datagram onto the message queue
	 * @param datagramBun
	 * @throws InterruptedException
	 */
	public void sendDatagram(DatagramBun datagramBun) throws InterruptedException {
		this.msgQueue.put(datagramBun);
	}

	/**
	 * 处理接收到的packetbean
	 * 
	 * @param bean
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void receivePacket(DatagramBun bean) throws UnknownHostException, IOException {
		switch (bean.getType()) {
		case "PrepareRequest":
			PrepareRequest prepareRequest = (PrepareRequest) bean.getPayload();
			onPrepare(prepareRequest.getPeerId(), prepareRequest.getInstance(), prepareRequest.getBallot());
			break;
		case "AcceptRequest":
			//AcceptRequest acceptRequest = gson.fromJson(bean.getPayload(), AcceptRequest.class);
			AcceptRequest acceptRequest = (AcceptRequest) bean.getPayload();
			onAccept(acceptRequest.getId(), acceptRequest.getInstance(), acceptRequest.getBallot(),
					acceptRequest.getValue());
			break;
		default:
			System.out.println("Unknown phase operation");
			break;
		}
	}

	/**
	 * handle prepare from proposer
	 * 
	 * @param instance
	 *            current instance
	 * @param ballot
	 *            prepare ballot
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void onPrepare(int peerId, int instance, int ballot) throws UnknownHostException, IOException {
		if (!instanceStatus.containsKey(instance)) {
			instanceStatus.put(instance, new Instance(ballot, null, 0));
			// 持久化到磁盘
			instancePersistence();
			prepareResponse(peerId, id, instance, true, 0, null);
		} else {
			Instance current = instanceStatus.get(instance);
			if (ballot > current.ballot) {
				current.ballot = ballot;
				// 持久化到磁盘
				instancePersistence();
				prepareResponse(peerId, id, instance, true, current.acceptedBallot, current.value);
			} else {
				prepareResponse(peerId, id, instance, false, current.ballot, null);
			}
		}
	}

	/**
	 * 
	 * @param id
	 *            accepter's id
	 * @param ok
	 *            ok or reject
	 * @param ab
	 *            accepted ballot
	 * @param av
	 *            accepted value
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	private void prepareResponse(int peerId, int id, int instance, boolean ok, int ab, Value av)
			throws UnknownHostException, IOException {
		DatagramBun bean = new DatagramBun("PrepareResponse",
				new PrepareResponse(id, instance, ok, ab, av));
		NodeInfo peer = getSpecInfoObect(peerId);
		this.client.sendTo(peer.getHost(), peer.getPort(), 
				this.objectSerialize.objectToObjectArray(new Datagram(bean, groupId, NodeType.PROPOSER)));
	}

	/**
	 * handle accept from proposer
	 * 
	 * @param instance
	 *            current instance
	 * @param ballot
	 *            accept ballot
	 * @param value
	 *            accept value
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void onAccept(int peerId, int instance, int ballot, Value value) throws UnknownHostException, IOException {
		this.logger.info("[Acceptor Responds to ACCEPT Request from Proposer: " + peerId + " Instance: " + instance + " Ballot: " + ballot + " Value: " + value.getPayload());
		if (!this.instanceStatus.containsKey(instance)) {
			acceptResponse(peerId, id, instance, false);
		} else {
			Instance current = this.instanceStatus.get(instance);
			if (ballot == current.ballot) {
				current.acceptedBallot = ballot;
				current.value = value;
				// 成功
				this.logger.info("[Acceptor responds to ACCEPT request succeed.]");
				this.acceptedValue.put(instance, value);
				if (!this.instanceStatus.containsKey(instance + 1)) {
					// multi-paxos 中的优化，省去了连续成功后的prepare阶段
					this.instanceStatus.put(instance + 1, new Instance(1, null, 0));
				}
				// 保存最后一次成功的instance的位置，用于proposer直接从这里开始执行
				this.lastInstanceId = instance;
				// 持久化到磁盘
				instancePersistence();
				acceptResponse(peerId, id, instance, true);
			} else {
				acceptResponse(peerId, id, instance, false);
			}
		}
		this.logger.info("[Acceptor responds to ACCEPT request end.]");
	}

	private void acceptResponse(int peerId, int id, int instance, boolean ok) throws UnknownHostException, IOException {
		NodeInfo nodeInfo = getSpecInfoObect(peerId);
		DatagramBun bean = new DatagramBun("AcceptResponse", new AcceptResponse(id, instance, ok));
		this.client.sendTo(nodeInfo.getHost(), nodeInfo.getPort(),
				this.objectSerialize.objectToObjectArray(new Datagram(bean, groupId, NodeType.PROPOSER)));
	}

	/**
	 * proposer从这获取最近的instance的id
	 * 
	 * @return
	 */
	public int getLastInstanceId() {
		return lastInstanceId;
	}

	/**
	 * 获取特定的info
	 * 
	 * @param key
	 * @return
	 */
	private NodeInfo getSpecInfoObect(int key) {
		for (NodeInfo each : this.proposers) {
			if (key == each.getId()) {
				return each;
			}
		}
		return null;
	}

	/**
	 * 在磁盘上存储instance
	 */
	private void instancePersistence() {
		if (!this.nodeConfiguration.isEnableDataPersistence())
			return;
		try {
			FileWriter fileWriter = new FileWriter(getInstanceFileAddr());
			fileWriter.write(gson.toJson(this.instanceStatus));
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * instance恢复
	 */
	private void instanceRecover() {
		if (!this.nodeConfiguration.isEnableDataPersistence())
			return;
		String data = FilesHandler.readFromFile(getInstanceFileAddr());
		if (data == null || data.length() == 0) {
			File file = new File(getInstanceFileAddr());
			if (!file.exists()) {
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return;
		}
		this.instanceStatus.putAll(gson.fromJson(data, new TypeToken<Map<Integer, Instance>>() {
		}.getType()));
		this.instanceStatus.forEach((key, value) -> {
			if (value.value != null)
				this.acceptedValue.put(key, value.value);
		});
	}
	
	/**
	 * 获取instance持久化的文件位置
	 * @return
	 */
	private String getInstanceFileAddr() {
		return this.nodeConfiguration.getDataDir() + "Accepter" + this.groupId + "_" + this.id + ".json";
	}

	public Map<Integer, Value> getAcceptedValue() {
		return acceptedValue;
	}

	public Map<Integer, Instance> getInstanceStatus() {
		return instanceStatus;
	}

	public int getId() {
		return id;
	}

	public NodeConfiguration getNodeConfiguration() {
		return nodeConfiguration;
	}

	public int getGroupId() {
		return groupId;
	}

}
