package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

import com.google.common.collect.ArrayListMultimap;

import java.util.Collections;

class DadkvsServerPaxos {

    int my_current_config;
    int my_current_priority;
    DadkvsServerState server_state;
    final int numPaxosServers = 3;

    HashMap<Integer, Pair> paxosLogs;

    public DadkvsServerPaxos(int config, DadkvsServerState state){
        my_current_priority = state.my_id;
        my_current_config = config;
        server_state = state;
        paxosLogs = new HashMap<>();
    }

    public void handleLeaderPaxos(int seqNum, int reqid){
        // SAVE LOG of this Consensus
        paxosLogs.put(seqNum, new Pair(reqid, my_current_priority));

        DadkvsPaxos.PhaseOneRequest.Builder phaseOneRequest  = DadkvsPaxos.PhaseOneRequest.newBuilder();
        while (server_state.i_am_leader){

            // SEND PHASE ONE REQUEST (Prepare)
            ArrayList<DadkvsPaxos.PhaseOneReply> phaseOne_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseOne_collector = new GenericResponseCollector<>(phaseOne_responses, numPaxosServers);
            phaseOneRequest.setPhase1Config(my_current_config).setSeqNum(seqNum).setPriority(my_current_priority);
            // Debug messages
            System.out.println("\n[handleLeaderPaxos] Request1.phase1Config = " + phaseOneRequest.getPhase1Config());
            System.out.println("[handleLeaderPaxos] Request1.seqNum = " + phaseOneRequest.getSeqNum());
            System.out.println("[handleLeaderPaxos] Request1.priority = " + phaseOneRequest.getPriority());

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
                    System.err.println("[handleLeaderPaxos] Reply2 - ERROR: Should not have received a different seqNumber!");
                }
                // Check accepted value
                if (phaseOne_reply.getAccepted()){ // Being accepted <=> my priority is higher
                    if (phaseOne_reply.getPhase1Value() != -1){ // "-1" means empty
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
                // Don't check priority, because "accepted" value already informs us of who has the higher priority
            } else {
                System.err.println("ERROR: did not receive any phase one responses");
                continue; // (HERE you try phase 1 again)
            }

            // SEND PHASE TWO REQUEST (Accept)
            DadkvsPaxos.PhaseTwoRequest.Builder phaseTwoRequest  = DadkvsPaxos.PhaseTwoRequest.newBuilder();
            ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwo_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phaseTwo_collector = new GenericResponseCollector<>(phaseTwo_responses, numPaxosServers);
            phaseTwoRequest.setPhase2Config(my_current_config).setSeqNum(seqNum).setPhase2Value(reqid).setPriority(my_current_priority);
            // Debug messages
            System.out.println("\n[handleLeaderPaxos] Request2.phase1Config = " + phaseTwoRequest.getPhase2Config());
            System.out.println("[handleLeaderPaxos] Request2.seqNum = " + phaseTwoRequest.getSeqNum());
            System.out.println("[handleLeaderPaxos] Request2.phase2Value = " + phaseTwoRequest.getPhase2Value());
            System.out.println("[handleLeaderPaxos] Request2.priority = " + phaseTwoRequest.getPriority());

            for (int i = my_current_config; i < numPaxosServers + my_current_config; i++) {
                if (i != server_state.my_id){
                    CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phaseTwo_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply>(phaseTwo_collector);
                    server_state.async_stubs[i].phaseTwo(phaseTwoRequest.build(), phaseTwo_observer);
                }
            }

            // RECEIVE PHASE TWO REPLY (Accepted)
            phaseTwo_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
            if (phaseTwo_responses.size() >= 1) {
                Iterator<DadkvsPaxos.PhaseTwoReply> phaseTwo_iterator = phaseTwo_responses.iterator();
                DadkvsPaxos.PhaseTwoReply phaseTwo_reply = phaseTwo_iterator.next();

                // Check seqNum value
                if (phaseTwo_reply.getSeqNum() != seqNum){
                    System.err.println("[handleLeaderPaxos] Reply2 - ERROR: Should not have received a different seqNumber!");
                }
                // Check accepted value
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

        synchronized(this){
            // Remove value from localOrderList, because it has already been decided by Paxos (so you don't try to propose it again)
            server_state.removeAndUpdateLocalOrder();
            server_state.notifyEveryone(); // Allows the next Paxos to start for the transaction with the new minLocalOrder
        }
        // Since the reqid was already accepted in Paxos, we assume the leader receives their own LearnRequest
        server_state.learnCounter.put(reqid, new Pair(seqNum, 1));

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

        // RECEIVE LEARN REPLY
        learn_collector.waitForTarget(1); // Don't care about the replies
        if (learn_responses.size() >= 1 ){
            System.out.println("[handleLeaderPaxos] Sending learn SUCCESS: At least one LearnRequest has been Replied");
        } else {
            System.err.println("ERROR: did not receive any learn replies");
        }
    }

    public DadkvsPaxos.PhaseOneReply handlePhaseOneReply(int p1config, int p1seqNum, int p1priority){
        boolean accepted;
        DadkvsPaxos.PhaseOneReply.Builder phaseOne_reply = DadkvsPaxos.PhaseOneReply.newBuilder();

        // Add log to ArrayList if it doesn't exist
        if (!paxosLogs.containsKey(p1seqNum)){
            paxosLogs.put(p1seqNum,new Pair(-1, p1priority));
        }
        // Update priority if incoming priority is higher
        if (paxosLogs.get(p1seqNum).getNum2() < p1priority){
            paxosLogs.get(p1seqNum).setNum2(p1priority);
        }
        // If I am leader, I don't accept any Prepare() from others
        if(server_state.i_am_leader){
            accepted = false;
        } else {
            accepted = true;
        }
        phaseOne_reply.setPhase1Config(Math.max(my_current_config, p1config))
                    .setSeqNum(p1seqNum)
                    .setAccepted(accepted)
                    .setPhase1Value(paxosLogs.get(p1seqNum).getNum1()) // Send value of the previous leader
                    .setPriority(paxosLogs.get(p1seqNum).getNum2()); // Send highest priority received
        // Debug messages
        System.out.println("\n[handlePhaseOneReply] Reply.phase1Config = " + phaseOne_reply.getPhase1Config());
        System.out.println("[handlePhaseOneReply] Reply.seqNum = " + phaseOne_reply.getSeqNum());
        System.out.println("[handlePhaseOneReply] Reply.accepted = " + phaseOne_reply.getAccepted());
        System.out.println("[handlePhaseOneReply] Reply.phase1Value = " + phaseOne_reply.getPhase1Value());
        System.out.println("[handlePhaseOneReply] Reply.priority = " + phaseOne_reply.getPriority());
        return phaseOne_reply.build();
    }

    public DadkvsPaxos.PhaseTwoReply handlePhaseTwoReply(int p2config, int p2seqNum, int p2value, int p2priority){
        boolean accepted;
        DadkvsPaxos.PhaseTwoReply.Builder phaseTwo_reply  = DadkvsPaxos.PhaseTwoReply.newBuilder();

        // Add log to ArrayList if it doesn't exist
        if (!paxosLogs.containsKey(p2seqNum) ){
            paxosLogs.put(p2seqNum,new Pair(-1, p2priority));
        }
        // If the incoming p2priority is higher, it means the PhaseTwoRequest was accepted and you can alread start sending LearnRequests to every server
        if(paxosLogs.get(p2seqNum).getNum2() <= p2priority){
            accepted = true;

            // Save value to be sent in PhaseOneReply
            paxosLogs.get(p2seqNum).setNum1(p2value);

            // SEND LEARN REQUEST
            DadkvsPaxos.LearnRequest.Builder learnRequest  = DadkvsPaxos.LearnRequest.newBuilder();
            ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
            GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector = new GenericResponseCollector<>(learn_responses, server_state.n_servers);
            learnRequest.setLearnconfig(my_current_config).setSeqNum(p2seqNum).setLearnvalue(p2value).setPriority(p2priority);
            for (int i = 0; i < server_state.n_servers; i++) {
                if (i != server_state.my_id){
                    CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer = new CollectorStreamObserver<DadkvsPaxos.LearnReply>(learn_collector);
                    server_state.async_stubs[i].learn(learnRequest.build(), learn_observer);
                }
            }

            // RECEIVE LEARN REPLY
            learn_collector.waitForTarget(1); // Don't care about the replies
            if (learn_responses.size() >= 1 ){
                System.out.println("[handlePhaseTwoReply] Sending learn SUCCESS: At least one LearnRequest has been Replied");
            } else {
                System.err.println("ERROR: did not receive any responses");
            }
            // Since the p2value was already accepted in Paxos, we assume this server receives their own LearnRequest
            if (!server_state.learnCounter.containsKey(p2value))
                server_state.learnCounter.put(p2value, new Pair(p2seqNum, 1));
            else{
                server_state.learnCounter.get(p2value).setNum2(server_state.learnCounter.get(p2value).getNum2() + 1);
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

    public DadkvsPaxos.LearnReply handleLearnReply(int lconfig, int lseqNum, int lvalue, int lpriority){
        boolean accepted = true;
        DadkvsPaxos.LearnReply.Builder learn_reply = DadkvsPaxos.LearnReply.newBuilder();
        learn_reply.setLearnconfig(Math.max(my_current_config, lconfig))
                    .setSeqNum(server_state.nextSeqNumber) // send your own nextSeqNumber (because: Why not??)
                    .setLearnaccepted(accepted);
        return learn_reply.build();
    }

}
