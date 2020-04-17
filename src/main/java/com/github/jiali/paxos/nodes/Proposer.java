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

import com.github.jiali.paxos.datagram.*;
import com.github.jiali.paxos.utils.PaxosClient;
import com.github.jiali.paxos.datagram.PrepareRequest;
import com.github.jiali.paxos.utils.ObjectSerialize;
import com.github.jiali.paxos.utils.ObjectSerializeImpl;

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

	// ready to submitToBuffer queue
	private BlockingQueue<Value> pendingSubmitQueue = new ArrayBlockingQueue<>(1);

	// submitted queue
	private BlockingQueue<Value> hasSubmitQueue = new ArrayBlockingQueue<>(1);

	// status of the last submitToBuffer
	private boolean isLastSubmitOk = false;

	// acceptor of the current node
	private Acceptor acceptor;

	// group id
	private int groupId;

	// message queue to store datagram
	private BlockingQueue<DatagramBun> msgQueue = new LinkedBlockingQueue<>();

	private BlockingQueue<DatagramBun> submitMsgQueue = new LinkedBlockingQueue<>();

	private ObjectSerialize objectSerialize = new ObjectSerializeImpl();

//	private Logger logger = Logger.getLogger("KV-Paxos");

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
			onAcceptResponse(acceptResponse.getId(), acceptResponse.getInstance(),
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
		this.pendingSubmitQueue.put(object);
		beforePrepare();
		Value value = this.hasSubmitQueue.take();
		return value;
	}

	/**
	 * actions before sending prepare to the quorum of acceptors:
	 * (1) set up instance
	 * (2) check if the previous instance is successfully committed.
	 * If yes, skip phase 1 and proceed.
	 * If no, start to prepare the prepare message.
	 */
	public void beforePrepare() {
		// 获取accepter最近的一次instance的id
		this.currentInstance = Math.max(this.currentInstance, acceptor.getMostRecentInstanceId());
		this.currentInstance++;
		Instance instance = new Instance(1, new HashSet<>(), null, 0, new HashSet<>(), false, ProposerState.READY);
		this.instanceState.put(this.currentInstance, instance);
		if (this.isLastSubmitOk == false) {
			// 执行完整的流程
			prepare(this.id, this.currentInstance, 1);
		} else {
			// multi-paxos 中的优化，直接accept
			instance.isSucc = true;
			accept(this.id, this.currentInstance, 1, this.pendingSubmitQueue.peek());
		}
	}

	/**
	 * 	 * 将prepare发送给所有的accepter，并设置超时。
	 *      * 如果超时，则判断阶段1是否完成，如果未完成，则ballot加一之后继续执行阶段一。
	 *      *
	 * 	 * Phase 1a: Prepare
	 *      * A Proposer creates a message, which we call a "Prepare",
	 *      * identified with a number n.
	 *      * Note that n is not the value to be proposed and maybe agreed on,
	 *      * but just a number which uniquely identifies this initial message by the proposer
	 *      * (to be sent to the acceptors).
	 *      * The number n must be greater than any number
	 *      * used in any of the previous Prepare messages by this Proposer.
	 *      * Then, it sends the Prepare message containing n to a Quorum of Acceptors.
	 *      * Note that the Prepare message only contains the number n
	 *      * (that is, it does not have to contain e.g. the proposed value, often denoted by v).
	 *      * The Proposer decides who is in the Quorum[how?].
	 *      *
	 *      * A Proposer should not initiate Paxos
	 *      * if it cannot communicate with at least a Quorum of Acceptors.
	 * @param id
	 * @param instance
	 * @param ballot
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
				// retry phase 1
				Instance current = instanceState.get(instance);
				if (current.state == ProposerState.PREPARE) {
					current.ballot++;
					prepare(id, instance, current.ballot);
				}
			}
		});
	}

	/**
	 * Phase 2a: Accept
	 *
	 * If a Proposer receives a majority of Promises from a Quorum of Acceptors,
	 * it needs to set a value v to its proposal.
	 * If any Acceptors had previously accepted any proposal,
	 * then they'll have sent their values to the Proposer,
	 * who now must set the value of its proposal, v,
	 * to the value associated with the highest proposal number reported by the Acceptors,
	 * let's call it z.
	 * If none of the Acceptors had accepted a proposal up to this point,
	 * then the Proposer may choose the value it originally wanted to propose, say x.
	 * The Proposer sends an Accept message, (n, v),
	 * to a Quorum of Acceptors with the chosen value for its proposal, v,
	 * and the proposal number n
	 * (which is the same as the number contained in the Prepare message previously sent to the Acceptors).
	 * So, the Accept message is either (n, v=z) or,
	 * in case none of the Acceptors previously accepted a value, (n, v=x).
	 * This Accept message should be interpreted as a "request",
	 * as in "Accept this proposal, please!".
	 * @param peerId
	 * @param instance
	 * @param ok
	 * @param ab
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
					Value myValue = this.pendingSubmitQueue.peek();
					current.value = myValue;
					current.isSucc = true;
				}
				accept(id, instance, current.ballot, current.value);
			}
		}
	}

	/**
	 * send sentAcceptRequest request to all acceptors, and set the ProposerState to ACCEPT
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
				// retry phase 2 if fail
				Instance current = instanceState.get(instance);
				if (current.state == ProposerState.ACCEPT) {
					current.ballot++;
					prepare(id, instance, current.ballot);
				}
			}
		});
	}

	/**
	 *
	 * @param peerId
	 * @param instance
	 * @param ok
	 * @throws InterruptedException
	 */
	public void onAcceptResponse(int peerId, int instance, boolean ok) throws InterruptedException {
		Instance current = this.instanceState.get(instance);
		if (current.state != ProposerState.ACCEPT)
			return;
		if (ok) {
			current.acceptSet.add(peerId);
			if (current.acceptSet.size() >= this.acceptorNo / 2 + 1) {
				// paxos ends
				done(instance);
				if (current.isSucc) {
					this.isLastSubmitOk = true;
					this.hasSubmitQueue.put(this.pendingSubmitQueue.take());
				} else {
					// this current instance has been occupied
					this.isLastSubmitOk = false;
					beforePrepare();
				}
			}
		}
	}

	/**
	 * paxos ends for the current voting
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
