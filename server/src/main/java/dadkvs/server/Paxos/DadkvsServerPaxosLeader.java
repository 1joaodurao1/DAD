package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dadkvs.server.*;

public class DadkvsServerPaxosLeader extends DadkvsServerPaxos {

	public DadkvsServerPaxosLeader(int config, DadkvsServerState state) {
		super(config, state);
	}

	public synchronized void handlePrepareAll(int startSeqNum){
		DadkvsPaxos.PrepareAllReply prepareAll_reply;
		while (server_state.isI_am_leader() && server_state.isLeaderInConfig()){
			prepareAll_reply = sendPrepareAllRequestAndWaitForReply(startSeqNum, my_current_priority, my_current_config);
			// Check accepted value
			if (!prepareAll_reply.getAccepted()){
				my_current_priority += server_state.getN_servers();
				continue; // Try PrepareAll Again with higher priority
			}
			// If accepted, save Logs
			for (DadkvsPaxos.PaxosLog paxosLog : prepareAll_reply.getPaxosLogsList()){
				if (paxosLog.getReqid() != -1){ // If it's not an empty consensus
					synchronized(server_state.getPaxosLogs()){
						server_state.setPaxosLog(paxosLog.getSeqNum(), paxosLog.getReqid(), paxosLog.getPriority(), paxosLog.getConfig());
					}
					server_state.removeByReqidLocalOrder(paxosLog.getReqid());
					server_state.addToReqidsDone(paxosLog.getReqid());
				}
			}
			break;
		}
	}

	public void handlePaxos(int seqNum, int reqid , int localOrder_copy) {
		int configLog = my_current_config, reqidToPropose = reqid, phase2result = 0;
		// Allow next transaction in multi paxos to start
		server_state.removeByReqidLocalOrder(reqid);
		server_state.notifyAllServerState();

		// SAVE LOG of this Consensus (reqid is "-1" because the value isn't accepted yet)
		synchronized(server_state.getPaxosLogs()){
			if (!server_state.getPaxosLogs().containsKey(seqNum))
				server_state.getPaxosLogs().put(seqNum, new Triplet(-1, -1, my_current_config));
		}

		// DO CONSENSUS number seqNum
		while (server_state.isI_am_leader()) {
			phase2result = handlePhase2(seqNum, reqidToPropose, localOrder_copy); // If this is 1 you have to try phaseOne again
			if (phase2result == -1){
				break; // OUTSIDE CONFIG
			} else if (phase2result == 0){
				break; // SUCCESS
			} else if (phase2result == 1){
				// Phase2 was rejected due to low priority
				handlePrepareAll(server_state.getNextSeqNumber());
			}
		}

		if (phase2result == -1){
			return; // I am not in the configuration for this consensus, another leader will do this consensus
		}

		// Since the reqid was already accepted in Paxos, we assume the leader receives
		// their own LearnRequest
		server_state.updateLearnCounter(reqidToPropose, seqNum);

		synchronized(server_state.getPaxosLogs()){
			configLog = server_state.getPaxosLogs().get(seqNum).getNum3();
		}

		// Inform other servers of PAXOS consensus result
		server_state.getLearner().sendLearnRequests(configLog, seqNum, reqidToPropose, my_current_priority);
	}

	public int handlePhase2(int seqNum, int reqid , int localOrder_copy) {
		int config;
		synchronized(server_state.getPaxosLogs()){
			config = server_state.getPaxosLogs().get(seqNum).getNum3(); // Update config in case the log has changed
			// Check if I'm in the config of this particular Paxos
			if (!(config <= server_state.getMy_id() && server_state.getMy_id() <= config + numPaxosServers - 1)){
				return -1; // GIVE UP: I am outside the config of this consensus
			}
			// Check if there is already a reqid to propose
			if (server_state.getPaxosLogs().get(seqNum).getNum1() != -1
				&& server_state.getPaxosLogs().get(seqNum).getNum1() != reqid){
				server_state.addLocalOrder(localOrder_copy, reqid); // Try again my value in another SeqNum
				reqid = server_state.getPaxosLogs().get(seqNum).getNum1();
			}
		}

		// SEND PHASE TWO REQUEST (Accept)
		DadkvsPaxos.PhaseTwoRequest.Builder phaseTwoRequest = DadkvsPaxos.PhaseTwoRequest.newBuilder();
		ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwo_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phaseTwo_collector = new GenericResponseCollector<>(
				phaseTwo_responses, numPaxosServers);
		phaseTwoRequest.setPhase2Config(config).setSeqNum(seqNum).setPhase2Value(reqid)
				.setPriority(my_current_priority);
		// Debug messages
		System.out.println("\n[handlePhase2] Request2.phase1Config = " + phaseTwoRequest.getPhase2Config());
		System.out.println("[handlePhase2] Request2.seqNum = " + phaseTwoRequest.getSeqNum());
		System.out.println("[handlePhase2] Request2.phase2Value = " + phaseTwoRequest.getPhase2Value());
		System.out.println("[handlePhase2] Request2.priority = " + phaseTwoRequest.getPriority());

		final CountDownLatch latch = new CountDownLatch(numPaxosServers - 1);
		final ExecutorService executor = Executors.newFixedThreadPool(numPaxosServers - 1);
		ArrayList<Integer> serversList = server_state.makeList(config, config + numPaxosServers);
		for (final int i : serversList) {
			if (i != server_state.getMy_id()) {
				executor.submit(() -> {
					try {
						CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phaseTwo_observer = new CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply>(
								phaseTwo_collector);
						server_state.getAsync_stubs()[i].phaseTwo(phaseTwoRequest.build(), phaseTwo_observer);
					} catch (RuntimeException e){
						System.out.println("[handlePhase2] RuntimeException = " + e.getMessage());
					} finally {
						latch.countDown();
					}
				});
			}
		}

		try {
			System.out.println("[handlePhase2] SeqNum = " + seqNum + " (WAITING for all threads)");
			latch.await();
		} catch (InterruptedException e){
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			System.out.println("[handlePhase2] SeqNum = " + seqNum + " (All threads DONE)");
			executor.shutdown();
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
			// Check config value
			if(phaseTwo_reply.getPhase2Config() != config){
				synchronized(server_state.getPaxosLogs()){ // Update config
					server_state.getPaxosLogs().get(seqNum).setNum3(phaseTwo_reply.getPhase2Config());
				}
				return 2; // Try Phase Two again with correct config
			}
			// Check accepted value
			if (phaseTwo_reply.getPhase2Accepted()) {
				return 0; // SUCCESS
			} else {
				return 1; // Try Prepare All again
			}
		} else {
			System.err.println("ERROR: did not receive any phase2 responses");
		}
		return 1; // Try Prepare All again
	}

	public DadkvsPaxos.PrepareAllReply sendPrepareAllRequestAndWaitForReply(int startSeqNum, int priority, int config){
		// SEND PREPARE ALL REQUEST
		DadkvsPaxos.PrepareAllRequest.Builder prepareAllRequest = DadkvsPaxos.PrepareAllRequest.newBuilder();
		ArrayList<DadkvsPaxos.PrepareAllReply> prepareAll_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PrepareAllReply> prepareAll_collector = new GenericResponseCollector<>(
				prepareAll_responses, numPaxosServers);
		prepareAllRequest.setStartSeqNum(startSeqNum).setPriority(priority).setConfig(config);
		// Debug messages
		System.out.println("\n[handlePrepareAll] PrepareAll.startSeqNum = " + prepareAllRequest.getStartSeqNum());
		System.out.println("[handlePrepareAll] PrepareAll.priority = " + prepareAllRequest.getPriority());
		System.out.println("[handlePrepareAll] PrepareAll.config = " + prepareAllRequest.getConfig());

		final CountDownLatch latch = new CountDownLatch(numPaxosServers - 1);
		final ExecutorService executor = Executors.newFixedThreadPool(numPaxosServers - 1);
		ArrayList<Integer> serversList = server_state.makeList(config, config + numPaxosServers);
		for (final int i : serversList) {
			if (i != server_state.getMy_id()) {
				executor.submit(() -> {
					try {
						CollectorStreamObserver<DadkvsPaxos.PrepareAllReply> prepareAll_observer = new CollectorStreamObserver<DadkvsPaxos.PrepareAllReply>(
								prepareAll_collector);
						server_state.getAsync_stubs()[i].prepareAll(prepareAllRequest.build(), prepareAll_observer);
					} catch (RuntimeException e){
						System.out.println("[handlePrepareAll] RuntimeException = " + e.getMessage());
					} finally {
						latch.countDown();
					}
				});
			}
		}

		try {
			System.out.println("[handlePrepareAll] StartSeqNum = " + startSeqNum + " (WAITING for all threads)");
			latch.await();
		} catch (InterruptedException e){
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			System.out.println("[handlePrepareAll] StartSeqNum = " + startSeqNum + " (All threads DONE)");
			executor.shutdown();
		}

		// RECEIVE PREPARE ALL REPLY
		prepareAll_collector.waitForTarget(1); // The majority is 2, so it's the leader plus 1
		if (prepareAll_responses.size() >= 1) {
			return prepareAll_responses.iterator().next();
		} else {
			System.err.println("ERROR: did not receive any prepare all responses");
			return DadkvsPaxos.PrepareAllReply.newBuilder().build();
		}
	}
}
