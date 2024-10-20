package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;

import dadkvs.server.*;

public class DadkvsServerPaxosLeader extends DadkvsServerPaxos {

	public DadkvsServerPaxosLeader(int config, DadkvsServerState state) {
		super(config, state);
	}

	public void handlePaxos(int seqNum, int reqid) {
		// SAVE LOG of this Consensus
		server_state.getPaxosLogs().put(seqNum, new Pair(reqid, my_current_priority));

		// DO CONSENSUS number seqNum
		while (server_state.isI_am_leader()) {
			reqid = handlePhase1(seqNum, reqid);

			if(handlePhase2(seqNum, reqid)){ // If this is false you have to try phaseOne again
				break; // SUCCESS
			}
		}

		// Since the reqid was already accepted in Paxos, we assume the leader receives
		// their own LearnRequest
		server_state.updateLearnCounter(reqid, seqNum);

		// Inform other servers of PAXOS consensus result
		server_state.getLearner().sendLearnRequests(my_current_config, seqNum, reqid, my_current_priority);
	}

	public int handlePhase1(int seqNum, int reqid) {
		DadkvsPaxos.PhaseOneRequest.Builder phaseOneRequest = DadkvsPaxos.PhaseOneRequest.newBuilder();
		while (server_state.isI_am_leader()) {
			// SEND PHASE ONE REQUEST (Prepare)
			ArrayList<DadkvsPaxos.PhaseOneReply> phaseOne_responses = new ArrayList<>();
			GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseOne_collector = new GenericResponseCollector<>(
					phaseOne_responses, numPaxosServers);
			phaseOneRequest.setPhase1Config(my_current_config).setSeqNum(seqNum).setPriority(my_current_priority);
			// Debug messages
			System.out.println("\n[handlePhase1] Request1.phase1Config = " + phaseOneRequest.getPhase1Config());
			System.out.println("[handlePhase1] Request1.seqNum = " + phaseOneRequest.getSeqNum());
			System.out.println("[handlePhase1] Request1.priority = " + phaseOneRequest.getPriority());

			for (int i = my_current_config; i < numPaxosServers + my_current_config; i++) {
				if (i != server_state.getMy_id()) {
					CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseOne_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseOneReply>(
							phaseOne_collector);
					server_state.getAsync_stubs()[i].phaseOne(phaseOneRequest.build(), phaseOne_observer);
				}
			}

			// RECEIVE PHASE ONE REPLY (Promise)
			phaseOne_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
			if (phaseOne_responses.size() >= 1) {
				Iterator<DadkvsPaxos.PhaseOneReply> phaseOne_iterator = phaseOne_responses.iterator();
				DadkvsPaxos.PhaseOneReply phaseOne_reply = phaseOne_iterator.next();

				// Check seqNum value
				if (phaseOne_reply.getSeqNum() != seqNum) {
					System.err.println("[handlePhase1] Reply1 - ERROR: Should not have received a different seqNumber!");
				}
				// Check accepted value
				if (phaseOne_reply.getAccepted()) { // Being accepted <=> my priority is higher
					if (phaseOne_reply.getPhase1Value() != -1) { // "-1" means empty
						// You have to adopt the value of the previous leader, if he left any value
						reqid = phaseOne_reply.getPhase1Value();
					}
					server_state.setPaxosLogsReqId(seqNum,reqid);
					break; // HERE YOU CONTINUE TO PHASE TWO
				} else {
					// HERE you try Phase One again (with a higher priority)
					this.my_current_priority += this.server_state.getN_servers();
					server_state.updatePaxosLogs(seqNum, this.my_current_priority);
				}
				// Don't check priority, because "accepted" value already informs us of who has
				// the higher priority
			} else {
				System.err.println("ERROR: did not receive any phase one responses");
				// (HERE you try phase 1 again)
			}
		}
		return reqid; // Could have been changed, so we need to give this value to phase 2
	}

	public boolean handlePhase2(int seqNum, int reqid) {
		// SEND PHASE TWO REQUEST (Accept)
		DadkvsPaxos.PhaseTwoRequest.Builder phaseTwoRequest = DadkvsPaxos.PhaseTwoRequest.newBuilder();
		ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwo_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phaseTwo_collector = new GenericResponseCollector<>(
				phaseTwo_responses, numPaxosServers);
		phaseTwoRequest.setPhase2Config(my_current_config).setSeqNum(seqNum).setPhase2Value(reqid)
				.setPriority(my_current_priority);
		// Debug messages
		System.out.println("\n[handlePhase2] Request2.phase1Config = " + phaseTwoRequest.getPhase2Config());
		System.out.println("[handlePhase2] Request2.seqNum = " + phaseTwoRequest.getSeqNum());
		System.out.println("[handlePhase2] Request2.phase2Value = " + phaseTwoRequest.getPhase2Value());
		System.out.println("[handlePhase2] Request2.priority = " + phaseTwoRequest.getPriority());

		for (int i = my_current_config; i < numPaxosServers + my_current_config; i++) {
			if (i != server_state.getMy_id()) {
				CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phaseTwo_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply>(
						phaseTwo_collector);
				server_state.getAsync_stubs()[i].phaseTwo(phaseTwoRequest.build(), phaseTwo_observer);
			}
		}

		// RECEIVE PHASE TWO REPLY (Accepted)
		phaseTwo_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
		if (phaseTwo_responses.size() >= 1) {
			Iterator<DadkvsPaxos.PhaseTwoReply> phaseTwo_iterator = phaseTwo_responses.iterator();
			DadkvsPaxos.PhaseTwoReply phaseTwo_reply = phaseTwo_iterator.next();

			// Check seqNum value
			if (phaseTwo_reply.getSeqNum() != seqNum) {
				System.err.println("[handlePhase2] Reply2 - ERROR: Should not have received a different seqNumber!");
			}
			// Check accepted value
			if (phaseTwo_reply.getPhase2Accepted()) {
				return true; // SUCCESS
			} else {
				// HERE you try Phase One again
				this.my_current_priority += this.server_state.getN_servers();
			}
		} else {
			System.err.println("ERROR: did not receive any phase2 responses");
		}
		return false; // Try Phase One again
	}
}
