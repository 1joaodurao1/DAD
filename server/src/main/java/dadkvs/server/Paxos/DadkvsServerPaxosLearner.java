package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

import dadkvs.server.*;

import com.google.common.collect.ArrayListMultimap;

import java.util.Collections;

public class DadkvsServerPaxosLearner extends DadkvsServerPaxos {

	public DadkvsServerPaxosLearner(int config, DadkvsServerState state) {
		super(config, state);
	}

}
