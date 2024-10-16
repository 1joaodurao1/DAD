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

	public DadkvsPaxos.LearnReply handleLearnReply(int lconfig, int lseqNum, int lvalue, int lpriority) {
		boolean accepted = true;
		DadkvsPaxos.LearnReply.Builder learn_reply = DadkvsPaxos.LearnReply.newBuilder();
		learn_reply.setLearnconfig(Math.max(my_current_config, lconfig))
				.setSeqNum(server_state.getNextSeqNumber()) // send your own nextSeqNumber (because: Why not??)
				.setLearnaccepted(accepted);
		return learn_reply.build();
	}


}
