package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;

import dadkvs.server.*;


public class DadkvsServerPaxosLearner extends DadkvsServerPaxos {

	public DadkvsServerPaxosLearner(int config, DadkvsServerState state) {
		super(config, state);
	}

	public void sendLearnRequests(int config, int seqNum, int reqid, int priority){
		// SEND LEARN REQUEST
		DadkvsPaxos.LearnRequest.Builder learnRequest = DadkvsPaxos.LearnRequest.newBuilder();
		ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector = new GenericResponseCollector<>(learn_responses,
				server_state.getN_servers());
		learnRequest.setLearnconfig(config).setSeqNum(seqNum).setLearnvalue(reqid).setPriority(priority);

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
			System.out.println("[sendLearnRequests] Sending learn SUCCESS: At least one LearnRequest has been Replied");
		} else {
			System.err.println("ERROR: did not receive any learn replies");
		}
	}

	public DadkvsPaxos.LearnReply handleLearnRequest(int lconfig, int lseqNum, int lreqid, int lpriority) {
		boolean accepted = true;
		if (server_state.getNextSeqNumber() > lseqNum){
            // This request has already been processed
            System.out.println("\n[handleLearnRequest] Ignored: nextSeqNumber " + server_state.getNextSeqNumber() + " is HIGHER than the seqNumber received " + lseqNum);
        } else {
			server_state.updateLearnCounter(lreqid, lseqNum);
        }
		synchronized(server_state.getPaxosLogs()){
			if(!server_state.getPaxosLogs().containsKey(lseqNum)){
				server_state.getPaxosLogs().put(lseqNum, new Triplet(lreqid, lpriority, lconfig));
			}
			else{
				server_state.getPaxosLogs().get(lseqNum).setNum1(lreqid);
				server_state.getPaxosLogs().get(lseqNum).setNum2(lpriority);
				server_state.getPaxosLogs().get(lseqNum).setNum3(lconfig);
			}
		}
		// The reply is useless, but Grpc needs it to work
		DadkvsPaxos.LearnReply.Builder learn_reply = DadkvsPaxos.LearnReply.newBuilder();
		learn_reply.setLearnconfig(Math.max(my_current_config, lconfig))
				.setSeqNum(lseqNum)
				.setLearnaccepted(accepted);
		return learn_reply.build();
	}


}
