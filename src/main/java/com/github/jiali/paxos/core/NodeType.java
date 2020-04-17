package com.github.jiali.paxos.core;

import java.io.Serializable;

public enum NodeType implements Serializable {
	PROPOSER, ACCEPTER, LEARNER, SERVER
}
