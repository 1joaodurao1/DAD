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
		DadkvsPaxos.PhaseOneReply.Builder phaseOne_reply = DadkvsPaxos.PhaseOneReply.newBuilder();
		// If I am leader, I don't accept any Prepare() from others
		accepted = !server_state.isI_am_leader();

		// ALL accesses to paxosLogs need to be syncronized (for MultiPaxos)
		synchronized(this){
			server_state.updatePaxosLogs(p1seqNum, p1priority);
			phaseOne_reply.setPhase1Config(Math.max(my_current_config, p1config)) // TODO: Change this for Step4
					.setSeqNum(p1seqNum)
					.setAccepted(accepted)
					.setPhase1Value(server_state.getPaxosLogs().get(p1seqNum).getNum1()) // Send value of the previous leader
					.setPriority(server_state.getPaxosLogs().get(p1seqNum).getNum2()); // Send highest priority received
		}
		// Debug messages
		System.out.println("\n[handlePhaseOneRequest] Reply.phase1Config = " + phaseOne_reply.getPhase1Config());
		System.out.println("[handlePhaseOneRequest] Reply.seqNum = " + phaseOne_reply.getSeqNum());
		System.out.println("[handlePhaseOneRequest] Reply.accepted = " + phaseOne_reply.getAccepted());
		System.out.println("[handlePhaseOneRequest] Reply.phase1Value = " + phaseOne_reply.getPhase1Value());
		System.out.println("[handlePhaseOneRequest] Reply.priority = " + phaseOne_reply.getPriority());
		return phaseOne_reply.build();
	}

	public DadkvsPaxos.PhaseTwoReply handlePhaseTwoRequest(int p2config, int p2seqNum, int p2value, int p2priority) {
		boolean accepted;
		DadkvsPaxos.PhaseTwoReply.Builder phaseTwo_reply = DadkvsPaxos.PhaseTwoReply.newBuilder();

		// ALL accesses to paxosLogs need to be syncronized (for MultiPaxos)
		synchronized(this){
			// If the incoming p2priority is equal or higher, it means the PhaseTwoRequest was accepted
			accepted = server_state.updatePaxosLogs(p2seqNum, p2priority);
		}
		if (accepted){
			// Inform other servers of PAXOS consensus result
			server_state.getLearner().sendLearnRequests(my_current_config, p2seqNum, p2value, my_current_priority);

			// Since the p2value was already accepted in Paxos, we assume this server
			// receives their own LearnRequest
			server_state.updateLearnCounter(p2value, p2seqNum);
		}

		phaseTwo_reply.setPhase2Config(Math.max(my_current_config, p2config)) // TODO: Change this for Step4
				.setSeqNum(p2seqNum)
				.setPhase2Accepted(accepted);
		// Debug messages
		System.out.println("\n[handlePhaseTwoRequest] Reply.phase2Config = " + phaseTwo_reply.getPhase2Config());
		System.out.println("[handlePhaseTwoRequest] Reply.seqNum = " + phaseTwo_reply.getSeqNum());
		System.out.println("[handlePhaseTwoRequest] Reply.phase2Accepted = " + phaseTwo_reply.getPhase2Accepted());

		return phaseTwo_reply.build();
	}

}
