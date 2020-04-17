package com.github.jiali.paxos.datagram;

import java.io.Serializable;

import com.github.jiali.paxos.nodes.NodeType;

public class Datagram implements Serializable {

	private DatagramBun datagramBun;
	private int groupId;
	private NodeType nodeType;
	public Datagram(DatagramBun datagramBun, int groupId, NodeType nodeType) {
		super();
		this.datagramBun = datagramBun;
		this.groupId = groupId;
		this.nodeType = nodeType;
	}
	public DatagramBun getDatagramBun() {
		return datagramBun;
	}
	public void setDatagramBun(DatagramBun datagramBun) {
		this.datagramBun = datagramBun;
	}
	public int getGroupId() {
		return groupId;
	}
	public NodeType getNodeType() {
		return nodeType;
	}
}
