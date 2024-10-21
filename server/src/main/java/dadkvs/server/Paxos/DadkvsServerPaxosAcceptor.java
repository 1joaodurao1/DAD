package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

import dadkvs.server.*;


public class DadkvsServerPaxosAcceptor extends DadkvsServerPaxos {

	public DadkvsServerPaxosAcceptor(int config, DadkvsServerState state) {
		super(config, state);
	}

	public DadkvsPaxos.PhaseOneReply handlePhaseOneRequest(int p1config, int p1seqNum, int p1priority) {
		boolean accepted;
		int valueReply = -1, priorityReply = p1priority, configReply = p1config;
		DadkvsPaxos.PhaseOneReply.Builder phaseOne_reply = DadkvsPaxos.PhaseOneReply.newBuilder();
		// If I am leader, I don't accept any Prepare()s from others
		accepted = !server_state.isI_am_leader();

		synchronized(server_state.getPaxosLogs()){
			// Add log to Map if it doesn't exist
			if (!server_state.getPaxosLogs().containsKey(p1seqNum)){
				server_state.getPaxosLogs().put(p1seqNum, new Triplet(-1, p1priority, p1config));
			}
			// Update priority if incoming priority is higher
			if (server_state.getPaxosLogs().get(p1seqNum).getNum2() <= p1priority){
				server_state.getPaxosLogs().get(p1seqNum).setNum2(p1priority); // Update highest priority
			}
			valueReply 		= server_state.getPaxosLogs().get(p1seqNum).getNum1(); // Send value of the previous leader ("-1" means there is no value)
			priorityReply 	= server_state.getPaxosLogs().get(p1seqNum).getNum2(); // Send highest priority received
			configReply 	= server_state.getPaxosLogs().get(p1seqNum).getNum3(); // Send config of first value accepted
		}
		phaseOne_reply.setPhase1Config(configReply)
				.setSeqNum(p1seqNum)
				.setAccepted(accepted)
				.setPhase1Value(valueReply)
				.setPriority(priorityReply);

		// Debug messages
		System.out.println("\n[handlePhaseOneRequest] Reply.phase1Config = " + phaseOne_reply.getPhase1Config());
		System.out.println("[handlePhaseOneRequest] Reply.seqNum = " + phaseOne_reply.getSeqNum());
		System.out.println("[handlePhaseOneRequest] Reply.accepted = " + phaseOne_reply.getAccepted());
		System.out.println("[handlePhaseOneRequest] Reply.phase1Value = " + phaseOne_reply.getPhase1Value());
		System.out.println("[handlePhaseOneRequest] Reply.priority = " + phaseOne_reply.getPriority());
		return phaseOne_reply.build();
	}

	public DadkvsPaxos.PhaseTwoReply handlePhaseTwoRequest(int p2config, int p2seqNum, int p2value, int p2priority) {
		boolean accepted = false, duplicated = false;
		DadkvsPaxos.PhaseTwoReply.Builder phaseTwo_reply = DadkvsPaxos.PhaseTwoReply.newBuilder();

		if (server_state.getLearnCounter().containsKey(p2value) && server_state.getLearnCounter().get(p2value).getNum1() != p2seqNum){
			duplicated = true; // In this case, the leader has to stop proposing this value
		} else {
			synchronized(server_state.getPaxosLogs()){
				// Add log to Map if it doesn't exist
				if (!server_state.getPaxosLogs().containsKey(p2seqNum)){
					server_state.getPaxosLogs().put(p2seqNum, new Triplet(-1, p2priority, p2config));
				}
				// Save p2value (reqid) if incoming priority is higher
				if (server_state.getPaxosLogs().get(p2seqNum).getNum2() <= p2priority){
					server_state.getPaxosLogs().get(p2seqNum).setNum1(p2value);
					accepted = true;
				}
				p2config = server_state.getPaxosLogs().get(p2seqNum).getNum3(); // Send config of first value accepted
			}
		}

		if (!duplicated && accepted) {
			// Inform other servers of PAXOS consensus result
			server_state.getLearner().sendLearnRequests(p2config, p2seqNum, p2value, p2priority);

			// Since the p2value was already accepted in Paxos, we assume this server
			// receives their own LearnRequest
			server_state.updateLearnCounter(p2value, p2seqNum);
		}

		phaseTwo_reply.setPhase2Config(p2config)
				.setSeqNum(p2seqNum)
				.setPhase2Accepted(accepted)
				.setIsDuplicated(duplicated);
		// Debug messages
		System.out.println("\n[handlePhaseTwoRequest] Reply.phase2Config = " + phaseTwo_reply.getPhase2Config());
		System.out.println("[handlePhaseTwoRequest] Reply.seqNum = " + phaseTwo_reply.getSeqNum());
		System.out.println("[handlePhaseTwoRequest] Reply.phase2Accepted = " + phaseTwo_reply.getPhase2Accepted());

		return phaseTwo_reply.build();
	}

}
