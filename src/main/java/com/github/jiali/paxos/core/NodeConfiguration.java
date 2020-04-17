package com.github.jiali.paxos.core;

import java.util.ArrayList;
import java.util.List;

public class NodeConfiguration {
	private List<NodeInfo> nodes = new ArrayList<>();
	private int myid;
	private int timeout;
	private int learningInterval;
	private String dataDir;
	private boolean enableDataPersistence;

	public NodeConfiguration() {
	}

	public List<NodeInfo> getNodes() {
		return nodes;
	}

	public void setNodes(List<NodeInfo> nodes) {
		this.nodes = nodes;
	}

	public int getMyid() {
		return myid;
	}

	public int getTimeout() {
		return timeout;
	}

	public String getDataDir() {
		return dataDir;
	}


	public boolean isEnableDataPersistence() {
		return enableDataPersistence;
	}


	public int getLearningInterval() {
		return learningInterval;
	}

	public void setLearningInterval(int learningInterval) {
		this.learningInterval = learningInterval;
	}

}
