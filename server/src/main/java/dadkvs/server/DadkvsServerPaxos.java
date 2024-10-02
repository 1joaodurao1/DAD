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
            phaseOneRequest.setPhase1config(my_current_config).setSeqNum(seqNum).setPriority(my_current_priority);
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
            phaseTwoRequest.setPhase2config(my_current_config).setSeqNum(seqNum).setPhase2value(reqid).setPriority(my_current_priority);
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
                if (phaseTwo_reply.getPhase2accepted()){
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

        
    }



}
