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
import com.github.jiali.paxos.utils.PaxosClient;
import com.github.jiali.paxos.datagram.AcceptRequest;
import com.github.jiali.paxos.utils.ObjectSerialize;
import com.github.jiali.paxos.utils.ObjectSerializeImpl;
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
		// recover from instance
		instanceRecover();
		new Thread(() -> {
			while (true) {
				try {
					DatagramBun msg = msgQueue.take();
					receiveDatagram(msg);
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
	 * receive incoming request (prepare or accept) and act on it acordinally
	 * 
	 * @param bean
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void receiveDatagram(DatagramBun bean) throws UnknownHostException, IOException {
		switch (bean.getType()) {
		case "PrepareRequest":
			PrepareRequest prepareRequest = (PrepareRequest) bean.getPayload();
			respondToPrepare(prepareRequest.getPeerId(), prepareRequest.getInstance(), prepareRequest.getBallot());
			break;
		case "AcceptRequest":
			//AcceptRequest acceptRequest = gson.fromJson(bean.getPayload(), AcceptRequest.class);
			AcceptRequest acceptRequest = (AcceptRequest) bean.getPayload();
			respondToAccept(acceptRequest.getId(), acceptRequest.getInstance(), acceptRequest.getBallot(),
					acceptRequest.getValue());
			break;
		default:
			System.out.println("Unknown phase operation");
			break;
		}
	}

	/**
	 * Phase 1b: Promise
	 * Any of the Acceptors waits for a Prepare message from any of the Proposers.
	 * If an Acceptor receives a Prepare message,
	 * the Acceptor must look at the identifier number n of the just received Prepare message.
	 * There are two cases.
	 * If n is higher than every previous proposal number received,
	 * from any of the Proposers, by the Acceptor,
	 * then the Acceptor must return a message, which we call a "Promise", to the Proposer,
	 * to ignore all future proposals having a number less than n.
	 * If the Acceptor accepted a proposal at some point in the past,
	 * it must include the previous proposal number, say m,
	 * and the corresponding accepted value, say w, in its response to the Proposer.
	 *
	 * If n is <= any previous proposal number received from any Proposer by the Acceptor,
	 * the Acceptor can ignore the received proposal.
	 * It does not have to answer in this case for Paxos to work.
	 * However, for the sake of optimization,
	 * sending a denial (Nack) response would tell the Proposer
	 * that it can stop its attempt to create consensus with proposal n.
	 * @param peerId
	 * @param instance
	 * @param ballot
	 * @throws IOException
	 */
	public void respondToPrepare(int peerId, int instance, int ballot) throws UnknownHostException, IOException {
		if (!instanceStatus.containsKey(instance)) {
			instanceStatus.put(instance, new Instance(ballot, null, 0));
			// write data to disk
			instancePersistence();
			prepareResponse(peerId, id, instance, true, 0, null);
		} else {
			Instance current = instanceStatus.get(instance);
			if (ballot > current.ballot) {
				current.ballot = ballot;
				// write data to disk
				instancePersistence();
				prepareResponse(peerId, id, instance, true, current.acceptedBallot, current.value);
			} else {
				prepareResponse(peerId, id, instance, false, current.ballot, null);
			}
		}
	}

	/**
	 * 
	 * @param id accepter's id
	 * @param ACK ok or reject
	 * @param acceptedBallot accepted ballot
	 * @param acceptedValue  accepted value
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	private void prepareResponse(int peerId, int id, int instance, boolean ACK, int acceptedBallot, Value acceptedValue)
			throws UnknownHostException, IOException {
		DatagramBun bean = new DatagramBun("PrepareResponse",
				new PrepareResponse(id, instance, ACK, acceptedBallot, acceptedValue));
		NodeInfo peer = getObjectById(peerId);
		this.client.sendTo(peer.getHost(), peer.getPort(), 
				this.objectSerialize.objectToObjectArray(new Datagram(bean, groupId, NodeType.PROPOSER)));
	}

	/**
	 * Phase 2b: Accepted
	 * If an Acceptor receives an Accept message, (n, v), from a Proposer,
	 * it must accept it if and only if it has not already promised
	 * (in Phase 1b of the Paxos protocol) to
	 * only consider proposals having an identifier greater than n.
	 * If the Acceptor has not already promised (in Phase 1b)
	 * to only consider proposals having an identifier greater than n,
	 * it should register the value v (of the just received Accept message)
	 * as the accepted value (of the Protocol),
	 * and send an Accepted message to the Proposer and every Learner
	 * (which can typically be the Proposers themselves).
	 * Else, it can ignore the Accept message or request.
	 *
	 * @param instance current instance
	 * @param ballot  accepted ballot
	 * @param value  accepted value
	 * @throws IOException
	 * @throws UnknownHostException
	 */
	public void respondToAccept(int peerId, int instance, int ballot, Value value) throws UnknownHostException, IOException {
		this.logger.info("Acceptor Responds to ACCEPT Request from Proposer: " + peerId + " Instance: " + instance + " Ballot: " + ballot + " Value: " + value.getPayload());
		if (!this.instanceStatus.containsKey(instance)) {
			acceptResponse(peerId, id, instance, false);
		} else {
			Instance current = this.instanceStatus.get(instance);
			if (ballot == current.ballot) {
				current.acceptedBallot = ballot;
				current.value = value;
				// successfully receive ballot, value
				this.logger.info("+++++++ Acceptor responds to ACCEPT request succeed.");
				this.acceptedValue.put(instance, value);
				if (!this.instanceStatus.containsKey(instance + 1)) {
					// Multi-Paxos when phase 1 can be skipped:
					// subsequent instances of the basic Paxos protocol (represented by I+1) use the same leader,
					// so the phase 1, which consist in the Prepare and Promise sub-phases, is skipped.
					// Note that the Leader should be stable, i.e. it should not crash or change.
					this.instanceStatus.put(instance + 1, new Instance(1, null, 0));
				}
				// save the last instance id so that the proposer can start from there.
				this.lastInstanceId = instance;
				// write data to disk
				instancePersistence();
				acceptResponse(peerId, id, instance, true);
			} else {
				acceptResponse(peerId, id, instance, false);
			}
		}
		this.logger.info("Acceptor responds to ACCEPT request end.");
	}

	private void acceptResponse(int peerId, int id, int instance, boolean ACK) throws UnknownHostException, IOException {
		NodeInfo nodeInfo = getObjectById(peerId);
		DatagramBun bean = new DatagramBun("AcceptResponse", new AcceptResponse(id, instance, ACK));
		this.client.sendTo(nodeInfo.getHost(), nodeInfo.getPort(),
				this.objectSerialize.objectToObjectArray(new Datagram(bean, groupId, NodeType.PROPOSER)));
	}

	/**
	 * proposer gets Most Recent InstanceId
	 * 
	 * @return
	 */
	public int getMostRecentInstanceId() {
		return lastInstanceId;
	}

	/**
	 * get object by id
	 * 
	 * @param key
	 * @return
	 */
	private NodeInfo getObjectById(int key) {
		for (NodeInfo each : this.proposers) {
			if (key == each.getId()) {
				return each;
			}
		}
		return null;
	}

	/**
	 * write instance to disk
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
	 * instance recovery
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
	 * write instance to disk
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
