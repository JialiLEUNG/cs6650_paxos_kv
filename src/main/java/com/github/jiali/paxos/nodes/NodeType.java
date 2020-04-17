package com.github.jiali.paxos.nodes;

import java.io.Serializable;

public enum NodeType implements Serializable {
	PROPOSER, ACCEPTER, LEARNER, SERVER
}
