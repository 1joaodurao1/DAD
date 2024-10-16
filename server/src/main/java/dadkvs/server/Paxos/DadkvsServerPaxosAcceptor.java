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

public class DadkvsServerPaxosAcceptor extends DadkvsServerPaxos {

	public DadkvsServerPaxosAcceptor(int config, DadkvsServerState state) {
		super(config, state);
	}

}
