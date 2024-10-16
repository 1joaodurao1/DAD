package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

import dadkvs.server.DadkvsServerPaxos;

public class DadkvsServerPaxosLeader extends DadkvsServerPaxos {

	public DadkvsServerPaxosLeader(int config, DadkvsServerState state) {
		super(config, state);
	}

	public void handleLeaderPaxos(int seqNum, int reqid) {
		// SAVE LOG of this Consensus
		paxosLogs.put(seqNum, new Pair(reqid, my_current_priority));

		DadkvsPaxos.PhaseOneRequest.Builder phaseOneRequest = DadkvsPaxos.PhaseOneRequest.newBuilder();
		while (server_state.i_am_leader) {

			// SEND PHASE ONE REQUEST (Prepare)
			ArrayList<DadkvsPaxos.PhaseOneReply> phaseOne_responses = new ArrayList<>();
			GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseOne_collector = new GenericResponseCollector<>(
					phaseOne_responses, numPaxosServers);
			phaseOneRequest.setPhase1Config(my_current_config).setSeqNum(seqNum).setPriority(my_current_priority);
			// Debug messages
			System.out.println("\n[handleLeaderPaxos] Request1.phase1Config = " + phaseOneRequest.getPhase1Config());
			System.out.println("[handleLeaderPaxos] Request1.seqNum = " + phaseOneRequest.getSeqNum());
			System.out.println("[handleLeaderPaxos] Request1.priority = " + phaseOneRequest.getPriority());

			for (int i = my_current_config; i < numPaxosServers + my_current_config; i++) {
				if (i != server_state.my_id) {
					CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseOne_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseOneReply>(
							phaseOne_collector);
					server_state.async_stubs[i].phaseOne(phaseOneRequest.build(), phaseOne_observer);
				}
			}

			// RECEIVE PHASE ONE REPLY (Promise)
			phaseOne_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
			if (phaseOne_responses.size() >= 1) {
				Iterator<DadkvsPaxos.PhaseOneReply> phaseOne_iterator = phaseOne_responses.iterator();
				DadkvsPaxos.PhaseOneReply phaseOne_reply = phaseOne_iterator.next();

				// Check seqNum value
				if (phaseOne_reply.getSeqNum() > seqNum) {
					System.err.println("[handleLeaderPaxos] Reply2 - ERROR: Should not have received a different seqNumber!");
				}
				// Check accepted value
				if (phaseOne_reply.getAccepted()) { // Being accepted <=> my priority is higher
					if (phaseOne_reply.getPhase1Value() != -1) { // "-1" means empty
						// You have to adopt the value of the previous leader, if he left any value
						reqid = phaseOne_reply.getPhase1Value();
					}
					paxosLogs.get(seqNum).setNum1(reqid);
					// HERE YOU CONTINUE TO PHASE TWO
				} else {
					// HERE you try Phase One again (with a higher priority)
					this.my_current_priority += this.server_state.n_servers;
					paxosLogs.get(seqNum).setNum2(this.my_current_priority);
					continue;
				}
				// Don't check priority, because "accepted" value already informs us of who has
				// the higher priority
			} else {
				System.err.println("ERROR: did not receive any phase one responses");
				continue; // (HERE you try phase 1 again)
			}

			// SEND PHASE TWO REQUEST (Accept)
			DadkvsPaxos.PhaseTwoRequest.Builder phaseTwoRequest = DadkvsPaxos.PhaseTwoRequest.newBuilder();
			ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwo_responses = new ArrayList<>();
			GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phaseTwo_collector = new GenericResponseCollector<>(
					phaseTwo_responses, numPaxosServers);
			phaseTwoRequest.setPhase2Config(my_current_config).setSeqNum(seqNum).setPhase2Value(reqid)
					.setPriority(my_current_priority);
			// Debug messages
			System.out.println("\n[handleLeaderPaxos] Request2.phase1Config = " + phaseTwoRequest.getPhase2Config());
			System.out.println("[handleLeaderPaxos] Request2.seqNum = " + phaseTwoRequest.getSeqNum());
			System.out.println("[handleLeaderPaxos] Request2.phase2Value = " + phaseTwoRequest.getPhase2Value());
			System.out.println("[handleLeaderPaxos] Request2.priority = " + phaseTwoRequest.getPriority());

			for (int i = my_current_config; i < numPaxosServers + my_current_config; i++) {
				if (i != server_state.my_id) {
					CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phaseTwo_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply>(
							phaseTwo_collector);
					server_state.async_stubs[i].phaseTwo(phaseTwoRequest.build(), phaseTwo_observer);
				}
			}

			// RECEIVE PHASE TWO REPLY (Accepted)
			phaseTwo_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
			if (phaseTwo_responses.size() >= 1) {
				Iterator<DadkvsPaxos.PhaseTwoReply> phaseTwo_iterator = phaseTwo_responses.iterator();
				DadkvsPaxos.PhaseTwoReply phaseTwo_reply = phaseTwo_iterator.next();

				// Check seqNum value
				if (phaseTwo_reply.getSeqNum() != seqNum) {
					System.err.println("[handleLeaderPaxos] Reply2 - ERROR: Should not have received a different seqNumber!");
				}
				// Check accepted value
				if (phaseTwo_reply.getPhase2Accepted()) {
					break; // SUCCESS
				} else {
					// HERE you try Phase One again
					this.my_current_priority += this.server_state.n_servers;
				}
			} else {
				System.err.println("ERROR: did not receive any phase2 responses");
			}
		}

		// Since the reqid was already accepted in Paxos, we assume the leader receives
		// their own LearnRequest
		// server_state.learnCounter.put(reqid, new Pair(seqNum, 1));
		if (!server_state.learnCounter.containsKey(reqid)) {
			server_state.learnCounter.put(reqid, new Pair(seqNum, 1));
		} else {
			server_state.learnCounter.get(reqid).setNum2(server_state.learnCounter.get(reqid).getNum2() + 1);
			server_state.notifyEveryone();
		}

		// SEND LEARN REQUEST
		DadkvsPaxos.LearnRequest.Builder learnRequest = DadkvsPaxos.LearnRequest.newBuilder();
		ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector = new GenericResponseCollector<>(learn_responses,
				server_state.n_servers);
		learnRequest.setLearnconfig(my_current_config).setSeqNum(seqNum).setLearnvalue(reqid)
				.setPriority(my_current_priority);

		for (int i = 0; i < server_state.n_servers; i++) {
			if (i != server_state.my_id) {
				CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer = new CollectorStreamObserver<DadkvsPaxos.LearnReply>(
						learn_collector);
				server_state.async_stubs[i].learn(learnRequest.build(), learn_observer);
			}
		}

		// RECEIVE LEARN REPLY
		learn_collector.waitForTarget(1); // Don't care about the replies
		if (learn_responses.size() >= 1) {
			System.out.println("[handleLeaderPaxos] Sending learn SUCCESS: At least one LearnRequest has been Replied");
		} else {
			System.err.println("ERROR: did not receive any learn replies");
		}
	}

}
