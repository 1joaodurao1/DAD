package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;

class DadkvsServerPaxos {

    int my_current_config;
    int my_current_priority;
    DadkvsServerState server_state;
    final int numPaxosServers = 3;
    int lastReqIdproposed = -1;

    public DadkvsServerPaxos(int config, DadkvsServerState state){
        my_current_priority = state.my_id;
        my_current_config = config;
        server_state = state;
    }

    public int handleLeaderPaxos(int seqNum, int reqid){

        DadkvsPaxos.PhaseOneRequest.Builder phaseOneRequest  = DadkvsPaxos.PhaseOneRequest.newBuilder();
        while (server_state.i_am_leader){
            // SEND PHASE ONE REQUEST (Prepare)
            ArrayList<DadkvsPaxos.PhaseOneReply> phaseOne_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseOne_collector = new GenericResponseCollector<>(phaseOne_responses, numPaxosServers);
            phaseOneRequest.setPhase1Config(my_current_config).setSeqNum(seqNum).setPriority(my_current_priority);
            for (int i = my_current_config; i < numPaxosServers + my_current_config; i++) {
                if (i != server_state.my_id){
                    CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseOne_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseOneReply>(phaseOne_collector);
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
                    // This leader has missed at least one consensus, you need to check which transaction requests were already processed
                    return phaseOne_reply.getSeqNum();
                }
                // Check accepted value
                if (phaseOne_reply.getAccepted()){
                    if (phaseOne_reply.getPhase1Value() != -1){ // "-1" means empty
                        reqid = phaseOne_reply.getPhase1Value();
                    }
                    // HERE YOU GO TO PHASE TWO
                } else {
                    // HERE you try Phase One again
                    this.my_current_priority += this.server_state.n_servers;
                    continue;
                }
                // Don't check priority, because we will try again anyway if we are not accepted
            } else {
                System.err.println("ERROR: did not receive any phase one responses");
                continue; // (try phase 1 again)
            }

            // SEND PHASE TWO REQUEST (Accept)
            DadkvsPaxos.PhaseTwoRequest.Builder phaseTwoRequest  = DadkvsPaxos.PhaseTwoRequest.newBuilder();
            ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwo_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phaseTwo_collector = new GenericResponseCollector<>(phaseTwo_responses, numPaxosServers);
            phaseTwoRequest.setPhase2Config(my_current_config).setSeqNum(seqNum).setPhase2Value(reqid).setPriority(my_current_priority);
            for (int i = my_current_config; i < numPaxosServers + my_current_config; i++) {
                if (i != server_state.my_id){
                    CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phaseTwo_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply>(phaseTwo_collector);
                    server_state.async_stubs[i].phaseTwo(phaseTwoRequest.build(), phaseTwo_observer);
                }
            }
            phaseTwo_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
            if (phaseTwo_responses.size() >= 1) {
                Iterator<DadkvsPaxos.PhaseTwoReply> phaseTwo_iterator = phaseTwo_responses.iterator();
                DadkvsPaxos.PhaseTwoReply phaseTwo_reply = phaseTwo_iterator.next();
                // Check seqNum
                if (phaseTwo_reply.getSeqNum() > seqNum){
                    // Probably not happening
                }
                if (phaseTwo_reply.getPhase2Accepted()){
                    break; // SUCCESS
                } else {
                    // HERE you try Phase One again
                    this.my_current_priority += this.server_state.n_servers;
                }
            } else {
                System.err.println("ERROR: did not receive any phase2 responses");
            }
        }

        // Remove value from localOrderList, because it has already been decided by Paxos (so you don't try to propose it again)
        server_state.removeAndUpdateLocalOrder();
        notifyAll(); // Allows the next Paxos to start for the new minLocalOrder

        // SEND LEARN REQUEST
        DadkvsPaxos.LearnRequest.Builder learnRequest  = DadkvsPaxos.LearnRequest.newBuilder();
        ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector = new GenericResponseCollector<>(learn_responses, server_state.n_servers);
        learnRequest.setLearnconfig(my_current_config).setSeqNum(seqNum).setLearnvalue(reqid).setPriority(my_current_priority);
        for (int i = 0; i < server_state.n_servers; i++) {
            if (i != server_state.my_id){
                CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer = new CollectorStreamObserver<DadkvsPaxos.LearnReply>(learn_collector);
                server_state.async_stubs[i].learn(learnRequest.build(), learn_observer);
            }
        }
        learn_collector.waitForTarget(1); // Don't care about the replies
        if (learn_responses.size() >= 1 ){
            System.out.println("Success: At least one LearnRequest has been Replied");
        } else {
            System.err.println("ERROR: did not receive any responses");
        }

        return seqNum + 1; // To update the nextSeqNumToDecide
    }

    public DadkvsPaxos.PhaseOneReply handlePhaseOneReply(int p1config, int p1seqNum, int p1priority){
        boolean accepted;
        DadkvsPaxos.PhaseOneReply.Builder phaseOne_reply = DadkvsPaxos.PhaseOneReply.newBuilder();
        if(server_state.nextSeqNumber <= p1seqNum){ // If the incoming seqNum is lower, it means the leader hasn't executed all the transactions already decided by previous consensus
            accepted = true;
        } else {
            accepted = false;
        }
        phaseOne_reply.setPhase1Config(Math.max(my_current_config, p1config))
                    .setSeqNum(Math.max(server_state.nextSeqNumber, p1seqNum))
                    .setAccepted(accepted)
                    .setPhase1Value(lastReqIdproposed)
                    .setPriority(-1); // Priority is ignored, so idk about this value
        return phaseOne_reply.build();
    }

    public DadkvsPaxos.PhaseTwoReply handlePhaseTwoReply(int p2config, int p2seqNum, int p2value, int p2priority){
        boolean accepted;
        DadkvsPaxos.PhaseTwoReply.Builder phaseTwo_reply  = DadkvsPaxos.PhaseTwoReply.newBuilder();
        if(server_state.nextSeqNumber <= p2seqNum){ // If the incoming seqNum is lower, it means the leader hasn't executed all the transactions already decided by previous consensus
            accepted = true;
            lastReqIdproposed = p2value; // Value to be sent in PhaseOneReply
        } else {
            accepted = false;
        }
        phaseTwo_reply.setPhase2Config(Math.max(my_current_config, p2config))
                    .setSeqNum(Math.max(server_state.nextSeqNumber, p2seqNum))
                    .setPhase2Accepted(accepted);
        return phaseTwo_reply.build();
    }

    public DadkvsPaxos.LearnReply handleLearnReply(int lconfig, int lseqNum, int lvalue, int lpriority){
        boolean accepted = true;
        DadkvsPaxos.LearnReply.Builder learn_reply = DadkvsPaxos.LearnReply.newBuilder();
        learn_reply.setLearnconfig(Math.max(my_current_config, lconfig))
                    .setSeqNum(server_state.nextSeqNumber) // send your own nextSeqNumber (because: Why not?)
                    .setLearnaccepted(accepted);
        return learn_reply.build();
    }

}
