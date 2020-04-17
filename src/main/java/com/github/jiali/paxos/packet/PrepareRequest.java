package com.github.jiali.paxos.packet;

import java.io.Serializable;

public class PrepareRequest implements Serializable {
	private int peerId;
	private int instance;
	private int ballot;
	public PrepareRequest(int peerId, int instance, int ballot) {
		super();
		this.peerId = peerId;
		this.instance = instance;
		this.ballot = ballot;
	}
	public int getPeerId() {
		return peerId;
	}
	public void setPeerId(int peerId) {
		this.peerId = peerId;
	}
	public int getInstance() {
		return instance;
	}
	public void setInstance(int instance) {
		this.instance = instance;
	}
	public int getBallot() {
		return ballot;
	}
	public void setBallot(int ballot) {
		this.ballot = ballot;
	}
	
}
