package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;

import com.google.common.collect.ArrayListMultimap;

import java.util.Collections;

class DadkvsServerPaxos {

    int my_current_config;
    int my_current_priority;
    DadkvsServerState server_state;
    final int numPaxosServers = 3;

    ArrayList<Pair> paxosLogs;

    public DadkvsServerPaxos(int config, DadkvsServerState state){
        my_current_priority = state.my_id;
        my_current_config = config;
        server_state = state;
        paxosLogs = new ArrayList<>();
    }

    public int handleLeaderPaxos(int seqNum, int reqid){
        paxosLogs.add(new Pair(reqid, my_current_priority)); // SAVE LOG

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

                // Check seqNum value, not needed
                if (phaseOne_reply.getSeqNum() > seqNum) {
                    // This leader has missed at least one consensus, you need to check which transaction requests were already processed
                    System.err.println("[handleLeaderPaxos] Reply2 - ERROR: Should not have received a different seqNumber!");
                }
                // Check accepted value
                if (phaseOne_reply.getAccepted()){ // Being accpted <=> my priority
                    if (phaseOne_reply.getPhase1Value() != -1){ // "-1" means empty
                        reqid = phaseOne_reply.getPhase1Value();
                    }
                    paxosLogs.get(seqNum).setNum1(reqid);
                    // HERE YOU GO TO PHASE TWO
                } else {
                    // HERE you try Phase One again
                    this.my_current_priority += this.server_state.n_servers;
                    paxosLogs.get(seqNum).setNum2(this.my_current_priority);
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
            phaseTwo_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
            if (phaseTwo_responses.size() >= 1) {
                Iterator<DadkvsPaxos.PhaseTwoReply> phaseTwo_iterator = phaseTwo_responses.iterator();
                DadkvsPaxos.PhaseTwoReply phaseTwo_reply = phaseTwo_iterator.next();
                // Check seqNum
                if (phaseTwo_reply.getSeqNum() != seqNum){
                    System.err.println("[handleLeaderPaxos] Reply2 - ERROR: Should not have received a different seqNumber!");
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

        synchronized(this){
            // Remove value from localOrderList, because it has already been decided by Paxos (so you don't try to propose it again)
            server_state.removeAndUpdateLocalOrder();
            notifyAll(); // Allows the next Paxos to start for the new minLocalOrder
        }

        // SEND LEARN REQUEST
        server_state.learnCounter.put(reqid, new Pair(seqNum, 1)); //since I am leader assume I already received one learn request 
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
            System.out.println("[handleLeaderPaxos] Success: At least one LearnRequest has been Replied");
        } else {
            System.err.println("ERROR: did not receive any responses");
        }

        return seqNum + 1; // To update the nextSeqNumToDecide
    }

    public DadkvsPaxos.PhaseOneReply handlePhaseOneReply(int p1config, int p1seqNum, int p1priority){
        boolean accepted;
        DadkvsPaxos.PhaseOneReply.Builder phaseOne_reply = DadkvsPaxos.PhaseOneReply.newBuilder();
        if (paxosLogs.size() <= p1seqNum){ // Add log to ArrayList if it doesn't exist
            paxosLogs.add(new Pair(-1, p1priority));
        }
        if (paxosLogs.get(p1seqNum).getNum2() < p1priority){ // Update priority if incoming priority is higher
            paxosLogs.get(p1seqNum).setNum2(p1priority);
        }
        if(server_state.nextSeqNumber <= p1seqNum){ // If the incoming seqNum is lower, it means the leader hasn't executed all the transactions already decided by previous consensus
            accepted = true;
        } else {
            accepted = false;
        }
        phaseOne_reply.setPhase1Config(Math.max(my_current_config, p1config))
                    .setSeqNum(Math.max(server_state.nextSeqNumber, p1seqNum))
                    .setAccepted(accepted)
                    .setPhase1Value(paxosLogs.get(p1seqNum).getNum1())
                    .setPriority(-1); // Priority is ignored, so idk about this value
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
        if(paxosLogs.get(p2seqNum).getNum2() <= p2priority){ // If the incoming p2seqNum is lower, it means the leader hasn't executed all the transactions already decided by previous consensus
            accepted = true;
            paxosLogs.get(p2seqNum).setNum1(p2value); // Value to be sent in PhaseOneReply
            // accepters goig to send learnRequest
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
            learn_collector.waitForTarget(1); // Don't care about the replies
            if (learn_responses.size() >= 1 ){
                System.out.println("[handlePhaseTwoReply] Success: At least one LearnRequest has been Replied");
            } else {
                System.err.println("ERROR: did not receive any responses");
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
                    .setSeqNum(server_state.nextSeqNumber) // send your own nextSeqNumber (because: Why not?)
                    .setLearnaccepted(accepted);
        return learn_reply.build();
    }

}
