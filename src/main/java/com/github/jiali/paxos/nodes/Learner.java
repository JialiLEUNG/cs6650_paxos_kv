package com.github.jiali.paxos.nodes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.github.jiali.paxos.utils.PaxosClient;
import com.github.jiali.paxos.datagram.LearnRequest;
import com.github.jiali.paxos.datagram.LearnResponse;
import com.github.jiali.paxos.datagram.Datagram;
import com.github.jiali.paxos.datagram.DatagramBun;
import com.github.jiali.paxos.datagram.Value;
import com.github.jiali.paxos.utils.ObjectSerialize;
import com.github.jiali.paxos.utils.ObjectSerializeImpl;

public class Learner {

	// learner's id
	private int id;

	// acceptor's number
	private int accepterNum;

	// learners
	private List<NodeInfo> learners;

	// temporary learning state: instanceid -> id -> value
	private Map<Integer, Map<Integer, Value>> tmpState = new HashMap<>();

	// learning state
	private Map<Integer, Value> state = new HashMap<>();

	// current instance
	private volatile int currentInstance = 1;

	// configuration of learner
	private NodeInfo my;

	/**
	 * node configuration
	 */
	private NodeConfiguration nodeConfiguration;

	/**
	 * acceptor of the current node
	 */
	private Acceptor acceptor;

	/**
	 * executor
	 */
	private PaxosCallback executor;

	/**
	 * fixed time thread
	 */
	private ScheduledExecutorService service = Executors.newScheduledThreadPool(1);


	// group id
	private int groupId;

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();
	
	private Logger logger = Logger.getLogger("KV-Paxos");
	
	// client
	private PaxosClient client;
	
	// message queue for storing messages
	private BlockingQueue<DatagramBun> msgQueue = new LinkedBlockingQueue<>();

	public Learner(int id, List<NodeInfo> learners, NodeInfo my, NodeConfiguration nodeConfiguration, Acceptor acceptor,
				   PaxosCallback executor, int groupId, PaxosClient client) {
		super();
		this.id = id;
		this.accepterNum = learners.size();
		this.learners = learners;
		this.my = my;
		this.acceptor = acceptor;
		this.executor = executor;
		this.nodeConfiguration = nodeConfiguration;
		this.groupId = groupId;
		this.client = client;
		service.scheduleAtFixedRate(() -> {
			// broadcast learn request
			sendRequest(this.id, this.currentInstance);
		} , nodeConfiguration.getLearningInterval(), nodeConfiguration.getLearningInterval(), TimeUnit.MILLISECONDS);
		new Thread(() -> {
			while (true) {
				try {
					DatagramBun msg = msgQueue.take();
					respondToDatagram(msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}
	
	/**
	 * append packet (datagram) to message queue
	 * @param bun
	 * @throws InterruptedException
	 */
	public void sendDatagram(DatagramBun bun) throws InterruptedException {
		this.msgQueue.put(bun);
	}

	/**
	 * receive learning datagram and act on it
	 * 
	 * @param bean
	 */
	public void respondToDatagram(DatagramBun bean) {
		switch (bean.getType()) {
		case "LearnRequest":
			//LearnRequest request = gson.fromJson(bean.getPayload(), LearnRequest.class);
			LearnRequest request = (LearnRequest) bean.getPayload();
			Value value = null;
			if (acceptor.getAcceptedValue().containsKey(request.getInstance()))
				value = acceptor.getAcceptedValue().get(request.getInstance());
			respondToRequest(request.getId(), request.getInstance(), value);
			break;
		case "LearnResponse":
			//LearnResponse response = gson.fromJson(bean.getPayload(), LearnResponse.class);
			LearnResponse response = (LearnResponse) bean.getPayload();
			respondToResponse(response.getId(), response.getInstance(), response.getValue());
			break;
		default:
			System.err.println("Unknown Type!");
			break;
		}
	}

	/**
	 * send learn request
	 * 
	 * @param instance
	 *
	 */
	private void sendRequest(int id, int instance) {
		//this.tmpState.remove(instance);
		DatagramBun datagramBun = new DatagramBun("LearnRequest", new LearnRequest(id, instance));
		byte[] data;
		try {
			data = this.objectSerialize.objectToObjectArray(new Datagram(datagramBun, this.groupId, NodeType.LEARNER));
			learners.forEach((info) -> {
				try {
					this.client.sendTo(info.getHost(), info.getPort(), data);
				} catch (IOException e) {
					//
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * respond to learn request
	 * 
	 * @param peerId
	 * @param instance
	 * @param value
	 */
	private void respondToRequest(int peerId, int instance, Value value) {
		NodeInfo peer = getLearnerById(peerId);
		try {
			DatagramBun datagramBun = new DatagramBun("LearnResponse", new LearnResponse(id, instance, value));
			this.client.sendTo(peer.getHost(), peer.getPort(),
					this.objectSerialize.objectToObjectArray(new Datagram(datagramBun, this.groupId, NodeType.LEARNER)));
//			this.logger.info("+++++++ Learner responds to LEARN request succeed.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * get learner by id
	 * 
	 * @param id
	 * @return
	 */
	private NodeInfo getLearnerById(int id) {
		for (NodeInfo each : this.learners) {
			if (each.getId() == id) {
				return each;
			}
		}
		return null;
	}

	/**
	 * respond to learn response request
	 * 
	 * @param peerId
	 * @param instance
	 * @param value
	 */
	private void respondToResponse(int peerId, int instance, Value value) {
		if (!this.tmpState.containsKey(instance)) {
			this.tmpState.put(instance, new HashMap<>());
		}
		if (value == null)
			return;
		Map<Integer, Value> map = this.tmpState.get(instance);
		map.put(peerId, value);
		Map<Value, Integer> count = new HashMap<>();
		map.forEach((k, v) -> {
			if (!count.containsKey(v)) {
				count.put(v, 1);
			} else {
				count.put(v, count.get(v) + 1);
			}
		});
		count.forEach((k, v) -> {
			if (v >= this.accepterNum / 2 + 1) {
				this.state.put(instance, k);
				// when learner learns successfully, sync the state at acceptor
				this.acceptor.getAcceptedValue().put(instance, k);
				Acceptor.Instance acceptInstance = this.acceptor.getInstanceStatus().get(instance);
				if (acceptInstance == null) {
					this.acceptor.getInstanceStatus().put(instance, new Acceptor.Instance(1, k, 1));
				} else {
					acceptInstance.setValue(k);
				}
				if (instance == currentInstance) {
					// use paxos executor
					this.logger.info("[Learner respond success] Server: " + peerId + " Instance: " + instance + " Succeed.") ;
					handleCallback(k);
					currentInstance++;
				}
			}
		});
	}
	
	/**
	 *  handle callback
	 * @param value
	 */
	private void handleCallback(Value value) {
		byte[] data = value.getPayload();
		Queue<byte[]> values;
		try {
			values = this.objectSerialize.byteArrayToObject(data, Queue.class);
			values.forEach(v -> {
				this.executor.callback(v);
			});
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
