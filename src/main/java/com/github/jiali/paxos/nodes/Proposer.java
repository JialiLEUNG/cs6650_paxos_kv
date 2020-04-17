package com.github.jiali.paxos.nodes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.github.jiali.paxos.datagram.*;
import com.github.jiali.paxos.utils.client.PaxosClient;
import com.github.jiali.paxos.datagram.PrepareRequest;
import com.github.jiali.paxos.utils.serializable.ObjectSerialize;
import com.github.jiali.paxos.utils.serializable.ObjectSerializeImpl;

public class Proposer {
	enum ProposerState {
		READY, PREPARE, ACCEPT, FINISH
	}

	class Instance {
		private int ballot;
		// a set for promise receive
		private Set<Integer> pSet;
		// value found after phase 1
		private Value value;
		// value's ballot
		private int myValueBallot;
		// accept set
		private Set<Integer> acceptSet;
		// is wantvalue doneValue
		private boolean isSucc;
		// state
		private ProposerState state;

		public Instance(int ballot, Set<Integer> pSet, Value value, int myValueBallot, Set<Integer> acceptSet,
						boolean isSucc, ProposerState state) {
			super();
			this.ballot = ballot;
			this.pSet = pSet;
			this.value = value;
			this.myValueBallot = myValueBallot;
			this.acceptSet = acceptSet;
			this.isSucc = isSucc;
			this.state = state;
		}
	}

	private Map<Integer, Instance> instanceState = new HashMap<>();

	// current instance
	private int currentInstance = 0;

	// proposer's id
	private int id;

	// acceptor's number
	private int acceptorNo;

	// acceptors
	private List<NodeInfo> acceptors;

	// my info
	private NodeInfo my;

	// timeout for each phase(ms)
	private int timeout;

	// 准备提交的状态
	private BlockingQueue<Value> readyToSubmitQueue = new ArrayBlockingQueue<>(1);

	// 成功提交的状态
	private BlockingQueue<Value> hasSummitQueue = new ArrayBlockingQueue<>(1);

	// 上一次的提交是否成功
	private boolean isLastSumbitSucc = false;

	// 本节点的accepter
	private Acceptor acceptor;

	// 组id
	private int groupId;

	// 消息队列，保存packetbean
	private BlockingQueue<DatagramBun> msgQueue = new LinkedBlockingQueue<>();

	private BlockingQueue<DatagramBun> submitMsgQueue = new LinkedBlockingQueue<>();

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();

	private Logger logger = Logger.getLogger("KV-Paxos");

	// 客户端
	private PaxosClient client;

	public Proposer(int id, List<NodeInfo> acceptors, NodeInfo my, int timeout, Acceptor acceptor, int groupId,
                    PaxosClient client) {
		this.id = id;
		this.acceptors = acceptors;
		this.acceptorNo = acceptors.size();
		this.my = my;
		this.timeout = timeout;
		this.acceptor = acceptor;
		this.groupId = groupId;
		this.client = client;
		new Thread(() -> {
			while (true) {
				try {
					DatagramBun msg = this.msgQueue.take();
					recvPacket(msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
		new Thread(() -> {
			while (true) {
				try {
					DatagramBun msg = this.submitMsgQueue.take();
					submit((Value) msg.getPayload());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	/**
	 * 向消息队列中插入packetbean
	 * 
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
	 * @throws InterruptedException
	 */
	public void recvPacket(DatagramBun bean) throws InterruptedException {
		switch (bean.getType()) {
		case "PrepareResponse":
			PrepareResponse prepareResponse = (PrepareResponse) bean.getPayload();
			onPrepareResponse(prepareResponse.getId(), prepareResponse.getInstance(),
					prepareResponse.isOk(), prepareResponse.getAb(), prepareResponse.getAv());
			break;
		case "AcceptResponse":
			AcceptResponse acceptResponse = (AcceptResponse) bean.getPayload();
			onAcceptResponce(acceptResponse.getId(), acceptResponse.getInstance(),
					acceptResponse.isOk());
			break;
		case "SubmitPacket":
			this.submitMsgQueue.add(bean);
			break;
		default:
			System.out.println("unknown type!!!");
			break;
		}
	}

	/**
	 * 客户端向proposer提交想要提交的状态
	 * 
	 * @param object
	 * @return
	 * @throws InterruptedException
	 */
	public Value submit(Value object) throws InterruptedException {
		this.readyToSubmitQueue.put(object);
		beforPrepare();
		Value value = this.hasSummitQueue.take();
		return value;
	}

	/**
	 * 
	 * 在prepare操作之前
	 */
	public void beforPrepare() {
		// 获取accepter最近的一次instance的id
		this.currentInstance = Math.max(this.currentInstance, acceptor.getLastInstanceId());
		this.currentInstance++;
		Instance instance = new Instance(1, new HashSet<>(), null, 0, new HashSet<>(), false, ProposerState.READY);
		this.instanceState.put(this.currentInstance, instance);
		if (this.isLastSumbitSucc == false) {
			// 执行完整的流程
			prepare(this.id, this.currentInstance, 1);
		} else {
			// multi-paxos 中的优化，直接accept
			instance.isSucc = true;
			accept(this.id, this.currentInstance, 1, this.readyToSubmitQueue.peek());
		}
	}

	/**

	 * @param instance
	 *            current instance
	 * @param ballot
	 *            prepare's ballot
	 */
	private void prepare(int id, int instance, int ballot) {
		this.instanceState.get(instance).state = ProposerState.PREPARE;
		try {
			DatagramBun bean = new DatagramBun("PrepareRequest", new PrepareRequest(id, instance, ballot));
			byte[] msg = this.objectSerialize.objectToObjectArray(new Datagram(bean, groupId, NodeType.ACCEPTER));
			this.acceptors.forEach((info) -> {
				try {
					this.client.sendTo(info.getHost(), info.getPort(), msg);
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		setTimeout(new TimerTask() {

			@Override
			public void run() {
				// retry phase 1 again!
				Instance current = instanceState.get(instance);
				if (current.state == ProposerState.PREPARE) {
					current.ballot++;
					prepare(id, instance, current.ballot);
				}
			}
		});
	}

	/**
	 * 接收到accepter对于prepare的回复
	 * 
	 * @param id
	 * @param instance
	 * @param ok
	 * @param ballot
	 * @param av
	 * @throws InterruptedException
	 */
	public void onPrepareResponse(int peerId, int instance, boolean ok, int ballot, Value av) {
		Instance current = this.instanceState.get(instance);
		if (current.state != ProposerState.PREPARE)
			return;
		if (ok) {
			current.pSet.add(peerId);
			if (ballot > current.myValueBallot && av != null) {
				current.myValueBallot = ballot;
				current.value = av;
				current.isSucc = false;
			}
			if (current.pSet.size() >= this.acceptorNo / 2 + 1) {
				if (current.value == null) {
					Value myValue = this.readyToSubmitQueue.peek();
					current.value = myValue;
					current.isSucc = true;
				}
				accept(id, instance, current.ballot, current.value);
			}
		}
	}

	/**
	 * 向所有的accepter发送accept，并设置状态。
	 * 
	 * @param id
	 * @param instance
	 * @param ballot
	 * @param value
	 */
	private void accept(int id, int instance, int ballot, Value value) {
		this.instanceState.get(instance).state = ProposerState.ACCEPT;
		try {
			DatagramBun bean = new DatagramBun("AcceptRequest", new AcceptRequest(id, instance, ballot, value));
			byte[] msg = this.objectSerialize.objectToObjectArray(new Datagram(bean, groupId, NodeType.ACCEPTER));
			this.acceptors.forEach((info) -> {
				try {
					this.client.sendTo(info.getHost(), info.getPort(), msg);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		setTimeout(new TimerTask() {
			@Override
			public void run() {
				// retry phase 2 again!
				Instance current = instanceState.get(instance);
				if (current.state == ProposerState.ACCEPT) {
					current.ballot++;
					prepare(id, instance, current.ballot);
				}
			}
		});
	}

	/**
	 * 接收到accepter返回的accept响应
	 * 
	 * @param peerId
	 * @param instance
	 * @param ok
	 * @throws InterruptedException
	 */
	public void onAcceptResponce(int peerId, int instance, boolean ok) throws InterruptedException {
		Instance current = this.instanceState.get(instance);
		if (current.state != ProposerState.ACCEPT)
			return;
		if (ok) {
			current.acceptSet.add(peerId);
			if (current.acceptSet.size() >= this.acceptorNo / 2 + 1) {
				// 流程结束
				done(instance);
				if (current.isSucc) {
					this.isLastSumbitSucc = true;
					this.hasSummitQueue.put(this.readyToSubmitQueue.take());
				} else {
					// 说明这个instance的id已经被占有
					this.isLastSumbitSucc = false;
					beforPrepare();
				}
			}
		}
	}

	/**
	 * 本次paxos选举结束
	 */
	public void done(int instance) {
		this.instanceState.get(instance).state = ProposerState.FINISH;
	}

	/**
	 * set timeout task
	 * 
	 * @param task
	 */
	private void setTimeout(TimerTask task) {
		new Timer().schedule(task, this.timeout);
	}
}
