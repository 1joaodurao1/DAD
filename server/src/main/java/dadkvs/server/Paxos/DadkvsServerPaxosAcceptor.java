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

	public DadkvsPaxos.PhaseOneReply handlePhaseOneReply(int p1config, int p1seqNum, int p1priority) {
		boolean accepted;
		DadkvsPaxos.PhaseOneReply.Builder phaseOne_reply = DadkvsPaxos.PhaseOneReply.newBuilder();

		// Add log to ArrayList if it doesn't exist
		if (!server_state.getPaxosLogs().containsKey(p1seqNum)) {
			server_state.getPaxosLogs().put(p1seqNum, new Pair(-1, p1priority));
		}
		// Update priority if incoming priority is higher
		if (server_state.getPaxosLogs().get(p1seqNum).getNum2() < p1priority) {
			server_state.getPaxosLogs().get(p1seqNum).setNum2(p1priority);
		}
		// If I am leader, I don't accept any Prepare() from others
		if (server_state.isI_am_leader()) {
			accepted = false;
		} else {
			accepted = true;
		}
		phaseOne_reply.setPhase1Config(Math.max(my_current_config, p1config))
				.setSeqNum(p1seqNum)
				.setAccepted(accepted)
				.setPhase1Value(server_state.getPaxosLogs().get(p1seqNum).getNum1()) // Send value of the previous leader
				.setPriority(server_state.getPaxosLogs().get(p1seqNum).getNum2()); // Send highest priority received
		// Debug messages
		System.out.println("\n[handlePhaseOneReply] Reply.phase1Config = " + phaseOne_reply.getPhase1Config());
		System.out.println("[handlePhaseOneReply] Reply.seqNum = " + phaseOne_reply.getSeqNum());
		System.out.println("[handlePhaseOneReply] Reply.accepted = " + phaseOne_reply.getAccepted());
		System.out.println("[handlePhaseOneReply] Reply.phase1Value = " + phaseOne_reply.getPhase1Value());
		System.out.println("[handlePhaseOneReply] Reply.priority = " + phaseOne_reply.getPriority());
		return phaseOne_reply.build();
	}

	public DadkvsPaxos.PhaseTwoReply handlePhaseTwoReply(int p2config, int p2seqNum, int p2value, int p2priority) {
		boolean accepted;
		DadkvsPaxos.PhaseTwoReply.Builder phaseTwo_reply = DadkvsPaxos.PhaseTwoReply.newBuilder();

		// Add log to ArrayList if it doesn't exist
		if (!server_state.getPaxosLogs().containsKey(p2seqNum)) {
			server_state.getPaxosLogs().put(p2seqNum, new Pair(-1, p2priority));
		}
		// If the incoming p2priority is higher, it means the PhaseTwoRequest was
		// accepted and you can alread start sending LearnRequests to every server
		if (server_state.getPaxosLogs().get(p2seqNum).getNum2() <= p2priority) {
			accepted = true;

			// Save value to be sent in PhaseOneReply
			server_state.getPaxosLogs().get(p2seqNum).setNum1(p2value);

			// SEND LEARN REQUEST
			DadkvsPaxos.LearnRequest.Builder learnRequest = DadkvsPaxos.LearnRequest.newBuilder();
			ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
			GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector = new GenericResponseCollector<>(learn_responses,
					server_state.getN_servers());
			learnRequest.setLearnconfig(my_current_config).setSeqNum(p2seqNum).setLearnvalue(p2value).setPriority(p2priority);
			for (int i = 0; i < server_state.getN_servers(); i++) {
				if (i != server_state.getMy_id()) {
					CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer = new CollectorStreamObserver<DadkvsPaxos.LearnReply>(
							learn_collector);
					server_state.getAsync_stubs()[i].learn(learnRequest.build(), learn_observer);
				}
			}

			// RECEIVE LEARN REPLY
			learn_collector.waitForTarget(1); // Don't care about the replies
			if (learn_responses.size() >= 1) {
				System.out.println("[handlePhaseTwoReply] Sending learn SUCCESS: At least one LearnRequest has been Replied");
			} else {
				System.err.println("ERROR: did not receive any responses");
			}
			// Since the p2value was already accepted in Paxos, we assume this server
			// receives their own LearnRequest
			if (!server_state.getLearnCounter().containsKey(p2value))
				server_state.getLearnCounter().put(p2value, new Pair(p2seqNum, 1));
			else {
				server_state.getLearnCounter().get(p2value).setNum2(server_state.getLearnCounter().get(p2value).getNum2() + 1);
				server_state.notifyEveryone();
			}
		} else {
			accepted = false;
		}
		phaseTwo_reply.setPhase2Config(Math.max(my_current_config, p2config))
				.setSeqNum(p2seqNum)
				.setPhase2Accepted(accepted);
		// Debug messages
		System.out.println("\n[handlePhaseTwoReply] Reply.phase2Config = " + phaseTwo_reply.getPhase2Config());
		System.out.println("[handlePhaseTwoReply] Reply.seqNum = " + phaseTwo_reply.getSeqNum());
		System.out.println("[handlePhaseTwoReply] Reply.phase2Accepted = " + phaseTwo_reply.getPhase2Accepted());

		return phaseTwo_reply.build();
	}

}
