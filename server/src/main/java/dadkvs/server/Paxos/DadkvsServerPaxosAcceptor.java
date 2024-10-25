package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

import dadkvs.server.*;


public class DadkvsServerPaxosAcceptor extends DadkvsServerPaxos {
	int startOfEmptySeqNums = 0;
	int priorityForInfinity = -1;

	public DadkvsServerPaxosAcceptor(int config, DadkvsServerState state) {
		super(config, state);
	}

	public DadkvsPaxos.PrepareAllReply handlePrepareAllRequest(int startSeqNum, int priority, int config) {
		boolean accepted;
		DadkvsPaxos.PrepareAllReply.Builder prepareAll_reply = DadkvsPaxos.PrepareAllReply.newBuilder();
		// If I am leader, I don't accept any Prepare()s from others
		accepted = !server_state.isI_am_leader();

		if (priorityForInfinity > priority){
			accepted = false;
		} else {
			synchronized(server_state.getPaxosLogs()){
				for (int i = startSeqNum; i < startOfEmptySeqNums; i++){
					DadkvsPaxos.PaxosLog.Builder paxosLog_builder = DadkvsPaxos.PaxosLog.newBuilder();
					if (server_state.getPaxosLogs().containsKey(i)){
						if (server_state.getPaxosLogs().get(i).getNum2() > priority){
							accepted = false; // Only accept if all priority values are bellow
							break;
						}
						paxosLog_builder.setSeqNum(i)
										.setReqid(server_state.getPaxosLogs().get(i).getNum1())
										.setPriority(server_state.getPaxosLogs().get(i).getNum2())
										.setConfig(server_state.getPaxosLogs().get(i).getNum3());
					} else {
						paxosLog_builder.setSeqNum(i).setReqid(-1).setPriority(-1).setConfig(-1);
					}
					prepareAll_reply.addPaxosLogs(paxosLog_builder.build());
				}
				if (accepted){ // Reserve all these consensus to only accept this priority
					for (int i = startSeqNum; i < startOfEmptySeqNums; i++){
						if (server_state.getPaxosLogs().containsKey(i)){
							server_state.getPaxosLogs().get(i).setNum2(priority);
						}
					}
					priorityForInfinity = priority;
				}
			}
		}
		prepareAll_reply.setAccepted(accepted);
		// Debug messages
		System.out.println("\n[handlePrepareAllRequest] PrepareAll.accepted = " + prepareAll_reply.getAccepted());
		System.out.println("[handlePrepareAllRequest] PrepareAll.paxoLogs = " + prepareAll_reply.getPaxosLogsList());
		return prepareAll_reply.build();
	}

	public DadkvsPaxos.PhaseTwoReply handlePhaseTwoRequest(int p2config, int p2seqNum, int p2value, int p2priority) {
		boolean accepted = false;
		DadkvsPaxos.PhaseTwoReply.Builder phaseTwo_reply = DadkvsPaxos.PhaseTwoReply.newBuilder();

		synchronized(server_state.getPaxosLogs()){
			// Add log to Map if it doesn't exist
			if (!server_state.getPaxosLogs().containsKey(p2seqNum)){
				server_state.getPaxosLogs().put(p2seqNum, new Triplet(-1, p2priority, p2config));

				if (getMy_current_config() > p2config){
					p2config = getMy_current_config(); // Here accept=false
				} else {
					accepted = true; // There was no previous Log and the config is >= than mine
				}

				if (priorityForInfinity != p2priority){
					accepted = false; // Leader needs to prepareAll again
				}
			} else {
				// Save p2value (reqid) if incoming priority is higher
				if (server_state.getPaxosLogs().get(p2seqNum).getNum2() == p2priority
					&& server_state.getPaxosLogs().get(p2seqNum).getNum3() == p2config){
					server_state.getPaxosLogs().get(p2seqNum).setNum1(p2value);
					server_state.addToReqidsDone(p2value);
					accepted = true;
				} else {
					// Here accept=false
					p2config = server_state.getPaxosLogs().get(p2seqNum).getNum3(); // Send config of first value accepted
				}
			}
		}

		if (accepted) {
			if(startOfEmptySeqNums <= p2seqNum){
				startOfEmptySeqNums = p2seqNum + 1;
			}
			// Inform other servers of PAXOS consensus result
			server_state.getLearner().sendLearnRequests(p2config, p2seqNum, p2value, p2priority);

			// Since the p2value was already accepted in Paxos, we assume this server
			// receives their own LearnRequest
			server_state.updateLearnCounter(p2value, p2seqNum);
		}

		phaseTwo_reply.setPhase2Config(p2config)
				.setSeqNum(p2seqNum)
				.setPhase2Accepted(accepted);
		// Debug messages
		System.out.println("\n[handlePhaseTwoRequest] Reply.phase2Config = " + phaseTwo_reply.getPhase2Config());
		System.out.println("[handlePhaseTwoRequest] Reply.seqNum = " + phaseTwo_reply.getSeqNum());
		System.out.println("[handlePhaseTwoRequest] Reply.phase2Accepted = " + phaseTwo_reply.getPhase2Accepted());

		return phaseTwo_reply.build();
	}

}
