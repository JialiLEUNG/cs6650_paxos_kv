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

import com.github.jiali.paxos.utils.client.PaxosClient;
import com.github.jiali.paxos.datagram.LearnRequest;
import com.github.jiali.paxos.datagram.LearnResponse;
import com.github.jiali.paxos.datagram.Datagram;
import com.github.jiali.paxos.datagram.DatagramBun;
import com.github.jiali.paxos.datagram.Value;
import com.github.jiali.paxos.utils.serializable.ObjectSerialize;
import com.github.jiali.paxos.utils.serializable.ObjectSerializeImpl;

public class Learner {

	// learner's id
	private int id;

	// acceptor's number
	private int accepterNum;

	// accepters
	private List<NodeInfo> learners;

	// 学习到的临时状态 instanceid -> id -> value
	private Map<Integer, Map<Integer, Value>> tmpState = new HashMap<>();

	// 学习的状态
	private Map<Integer, Value> state = new HashMap<>();

	// 学习到的instance
	private volatile int currentInstance = 1;

	// learner配置信息
	private NodeInfo my;

	/**
	 * 全局配置文件信息
	 */
	private NodeConfiguration nodeConfiguration;

	/**
	 * 当前节点的accepter
	 */
	private Acceptor acceptor;

	/**
	 * 定时线程
	 */
	private ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

	/**
	 * 状态执行者
	 */
	private PaxosCallback executor;

	// 组id
	private int groupId;

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();
	
	private Logger logger = Logger.getLogger("KV-Paxos");
	
	// 客户端
	private PaxosClient client;
	
	// 消息队列，保存packetbean
	private BlockingQueue<DatagramBun> msgQueue = new LinkedBlockingQueue<>();

	public Learner(int id, List<NodeInfo> learners, NodeInfo my, NodeConfiguration nodeConfiguration, Acceptor acceptor,
				   PaxosCallback executor, int groupId, PaxosClient client) {
		super();
		this.id = id;
		this.accepterNum = learners.size();
		this.learners = learners;
		this.my = my;
		this.nodeConfiguration = nodeConfiguration;
		this.acceptor = acceptor;
		this.executor = executor;
		this.groupId = groupId;
		this.client = client;
		service.scheduleAtFixedRate(() -> {
			// 广播学习请求
			sendRequest(this.id, this.currentInstance);
		} , nodeConfiguration.getLearningInterval(), nodeConfiguration.getLearningInterval(), TimeUnit.MILLISECONDS);
		new Thread(() -> {
			while (true) {
				try {
					DatagramBun msg = msgQueue.take();
					recvPacket(msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}
	
	/**
	 * 向消息队列中插入packetbean
	 * @param bean
	 * @throws InterruptedException
	 */
	public void sendPacket(DatagramBun bean) throws InterruptedException {
		this.msgQueue.put(bean);
	}

	/**
	 * 处理接收到的packetbean
	 * 
	 * @param bean
	 */
	public void recvPacket(DatagramBun bean) {
		switch (bean.getType()) {
		case "LearnRequest":
			//LearnRequest request = gson.fromJson(bean.getPayload(), LearnRequest.class);
			LearnRequest request = (LearnRequest) bean.getPayload();
			Value value = null;
			if (acceptor.getAcceptedValue().containsKey(request.getInstance()))
				value = acceptor.getAcceptedValue().get(request.getInstance());
			sendResponse(request.getId(), request.getInstance(), value);
			break;
		case "LearnResponse":
			//LearnResponse response = gson.fromJson(bean.getPayload(), LearnResponse.class);
			LearnResponse response = (LearnResponse) bean.getPayload();
			onResponse(response.getId(), response.getInstance(), response.getValue());
			break;
		default:
			System.err.println("Unknown Type!");
			break;
		}
	}

	/**
	 * 发送请求
	 * 
	 * @param instance
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
	 * 发送响应
	 * 
	 * @param peerId
	 *            对端的id
	 * @param instance
	 * @param value
	 */
	private void sendResponse(int peerId, int instance, Value value) {
		NodeInfo peer = getSpecLearner(peerId);
		try {
			DatagramBun datagramBun = new DatagramBun("LearnResponse", new LearnResponse(id, instance, value));
			this.client.sendTo(peer.getHost(), peer.getPort(),
					this.objectSerialize.objectToObjectArray(new Datagram(datagramBun, this.groupId, NodeType.LEARNER)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 获取id为特定值的learner信息
	 * 
	 * @param id
	 * @return
	 */
	private NodeInfo getSpecLearner(int id) {
		for (NodeInfo each : this.learners) {
			if (each.getId() == id) {
				return each;
			}
		}
		return null;
	}

	/**
	 * 响应返回值
	 * 
	 * @param peerId
	 * @param instance
	 * @param value
	 */
	private void onResponse(int peerId, int instance, Value value) {
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
				// 当learner学习成功时，让accepter也去同步这个状态
				this.acceptor.getAcceptedValue().put(instance, k);
				Acceptor.Instance acceptInstance = this.acceptor.getInstanceStatus().get(instance);
				if (acceptInstance == null) {
					this.acceptor.getInstanceStatus().put(instance, new Acceptor.Instance(1, k, 1));
				} else {
					acceptInstance.setValue(k);
				}
				if (instance == currentInstance) {
					// 调用paxos状态执行者
					this.logger.info("[Learner respond success] Server: " + peerId + " Instance: " + instance + " Succeed.") ;
					handleCallback(k);
					currentInstance++;
				}
			}
		});
	}
	
	/**
	 *  调用paxos状态执行者 
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
