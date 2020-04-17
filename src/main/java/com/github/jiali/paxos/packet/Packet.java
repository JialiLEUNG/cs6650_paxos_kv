package com.github.jiali.paxos.packet;

import java.io.Serializable;

import com.github.jiali.paxos.core.NodeType;

public class Packet implements Serializable {

	private PacketBean packetBean;
	private int groupId;
	private NodeType nodeType;
	public Packet(PacketBean packetBean, int groupId, NodeType nodeType) {
		super();
		this.packetBean = packetBean;
		this.groupId = groupId;
		this.nodeType = nodeType;
	}
	public PacketBean getPacketBean() {
		return packetBean;
	}
	public void setPacketBean(PacketBean packetBean) {
		this.packetBean = packetBean;
	}
	public int getGroupId() {
		return groupId;
	}
	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
	public NodeType getNodeType() {
		return nodeType;
	}
	public void setNodeType(NodeType nodeType) {
		this.nodeType = nodeType;
	}
}
