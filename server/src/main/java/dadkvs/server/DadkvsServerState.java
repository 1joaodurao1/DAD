package dadkvs.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import dadkvs.server.Paxos.*;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;

public class DadkvsServerState {
    boolean        i_am_leader;
    int            base_port;
    int            my_id;
    int            store_size;
    KeyValueStore  store;
    MainLoop       main_loop;
    Thread         main_loop_worker;
    // Stubs and communication
    ManagedChannel[]                                server_channels;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;
    final int                                       n_servers;
    // ConsoleClient commands
    int     debug_mode;
    boolean isFreezed;
    boolean isDelayed;
    // Variables to control GLOBAL order
    int                     nextSeqNumber;
    HashMap<Integer, Pair>  learnCounter;
    // Variables to control next transaction to propose (only relevant if I am leader)
    AtomicInteger       localOrderCounter;
    AtomicInteger       nextSeqNumbertoPropose;
    ArrayList<Integer>  localOrderList;
    // Class with handlers for Paxos execution
    DadkvsServerPaxosLeader leader;
    DadkvsServerPaxosAcceptor acceptor;
    DadkvsServerPaxosLearner learner;
    Map<Integer, Triplet> paxosLogs;


    public DadkvsServerState(int kv_size, int port, int myself, int servers,
                            DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] paxoStubs,
                            ManagedChannel[] channels) {
        i_am_leader = false;
        base_port = port;
        my_id = myself;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);

        server_channels = channels;
        async_stubs = paxoStubs;
        n_servers = servers;

        debug_mode = 0;
        isDelayed = false;
        isFreezed = false;

        nextSeqNumber = 0;
        learnCounter = new HashMap<>();

        localOrderCounter = new AtomicInteger(0);
        nextSeqNumbertoPropose = new AtomicInteger(0);
        localOrderList = new ArrayList<Integer>();

        leader = new DadkvsServerPaxosLeader(0,this);
        acceptor = new DadkvsServerPaxosAcceptor(0,this);
        learner = new DadkvsServerPaxosLearner(0,this);
        paxosLogs = Collections.synchronizedMap(new HashMap<>());

        main_loop = new MainLoop(this);
        main_loop_worker = new Thread (main_loop);
        main_loop_worker.start();
    }



    public boolean handleTransaction(int reqid, TransactionRecord record){
        int localOrder_copy = this.localOrderCounter.getAndIncrement();
        localOrderList.add(localOrder_copy);

        synchronized(this){
            // Stay in this loop until you receive 2 LearnRequests for your transaction and the seqNum they have is the next one in line (nextSeqNumber)
            while (!learnCounter.containsKey(reqid) || !(learnCounter.get(reqid).getNum1() == nextSeqNumber && learnCounter.get(reqid).getNum2() >= 2)){
                // Debug Messages
                System.out.println("\n[handleTRansaction] i_am_leader = " +  this.i_am_leader);
                if (localOrderList.size() > 0)
                    System.out.println("[handleTRansaction] min(LocalorderList) = " +  Collections.min(localOrderList));
                System.out.println("[handleTRansaction] localOrder_copy = " +  localOrder_copy);
                System.out.println("[handleTRansaction] nextSeqNumber = " +  nextSeqNumber);
                System.out.println("[handleTRansaction] localOrderList = " +  localOrderList);
                if (learnCounter.containsKey(reqid) ){ // This "if" is to avoid NULL pointer exceptions
                    System.out.println("[handleTRansaction]:SeqNumber of learnCounter " + learnCounter.get(reqid).getNum1());
                    System.out.println("[handleTRansaction]:Number of Learn Requests " + learnCounter.get(reqid).getNum2());
                }

                if (this.i_am_leader && isLeaderInConfig() && (localOrderList.size() > 0 && Collections.min(localOrderList) == localOrder_copy)){
                    System.out.println("[handleTRansaction] Im leader and starting paxos with localOrder_copy = " + localOrder_copy);
                    // We add learnCounter.size() because there could be Transactions that were already accepted in Paxos, but that haven't yet received
                    //  the 2nd LearnRequest, removed their learnCounter entry and, subsequently, incremented the nextSeqNum (retirei  + learnCounter.size())
                    this.leader.handlePaxos(nextSeqNumbertoPropose.getAndIncrement(), reqid , localOrder_copy);
                } else {
                    try { wait ();}
                    catch (InterruptedException e) {} // Ignore
                }
            }
            System.out.println("[handleTransaction] Im a learner and im going to commit with seqNumber = " + learnCounter.get(reqid).getNum1() + "and request id " + reqid);
            this.syncRemoveMinLocalOrder(localOrder_copy);
            boolean result = this.store.commit(record);
            if (result){
                if ( record.getPrepareKey() == 0) {
                    System.out.println("[handleTransaction] Im a learner and im going to commit a reconfig to configuration " + record.getPrepareKey());
                    learner.setMy_current_config(record.getPrepareKey());
                    leader.setMy_current_config(record.getPrepareKey());
                    acceptor.setMy_current_config(record.getPrepareKey());
                    // If im leader and change config, but i belong to next config i continue being a leader, else i stop being a leader
                    if ( i_am_leader && !isLeaderInConfig()){
                        i_am_leader = false;
                    }
                }
            }
            //[Going to remove this line for Step4] learnCounter.remove(reqid);
            nextSeqNumber++;
            notifyAll(); // Tell the next transaction to execute, if it's ready (i.e. received at least 2 LearnRequests)
            return result;
        }
    }

    public boolean isLeaderInConfig(){
        return leader.getMy_current_config() <= my_id && my_id <= leader.getMy_current_config() + n_servers - 1;
    }

    public void server_exit(){
        System.out.println("Quitting the process and exiting.");
        // Desconnecting the channels before exiting
        for (int i = 0; i < n_servers; i++) {
            if (i != my_id) { // Don't shutDown the channel to yourself, because it doesn't exist
                server_channels[i].shutdownNow();
            }
        }
        System.exit(0); // crashing the server
    }

    public void freezeServer(){
        synchronized (this) {
            while (this.isFreezed){
                System.out.println("I am freezed ");
                try { wait ();}
                catch (InterruptedException e) {} // Ignore
            }
        }
    }

    public void insertDelay(){
        Random random = new Random();
        int delay = random.nextInt(2500); // Random delay between 0 and 2500 ms

        // For debug purposes
        System.out.println("\n[insertDelay] Random delay: " + delay + " milliseconds.");

        try {
            Thread.sleep(delay); // Introduce the delay
        } catch (InterruptedException e) {
            e.printStackTrace(); // Handle interruption during sleep
        }
    }

    public void handleDebug(int mode) {
        this.debug_mode = mode;

        switch(mode) {
            case 1:
                System.out.println("\n[handleDebug] System shutting down");
            case 2:
                this.isFreezed = true;
                break;
            case 3:
                this.isFreezed = false;
                synchronized(this){
                    notifyAll();
                }
                break;
            case 4:
                this.isDelayed = true;
                break;
            case 5:
                this.isDelayed = false;
                break;
            default:
                System.err.println("\n[handleDebug] ERROR: Default mode not known");
                break;
        }
    }

    public boolean isI_am_leader() {
        return i_am_leader;
    }

    public void setI_am_leader(boolean i_am_leader) {
        this.i_am_leader = i_am_leader;
    }

    public int getN_servers() {
        return n_servers;
    }

    public int getMy_id() {
        return my_id;
    }

    public void setMy_id(int my_id) {
        this.my_id = my_id;
    }

    public ManagedChannel[] getServer_channels() {
        return server_channels;
    }

    public void setServer_channels(ManagedChannel[] server_channels) {
        this.server_channels = server_channels;
    }

    public DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] getAsync_stubs() {
        return async_stubs;
    }

    public void setAsync_stubs(DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs) {
        this.async_stubs = async_stubs;
    }

    public int getDebug_mode() {
        return debug_mode;
    }

    public void setDebug_mode(int debug_mode) {
        this.debug_mode = debug_mode;
    }

    public boolean isFreezed() {
        return isFreezed;
    }

    public void setFreezed(boolean freezed) {
        isFreezed = freezed;
    }

    public boolean isDelayed() {
        return isDelayed;
    }

    public void setDelayed(boolean delayed) {
        isDelayed = delayed;
    }

    public int getNextSeqNumber() {
        return nextSeqNumber;
    }

    public void setNextSeqNumber(int nextSeqNumber) {
        this.nextSeqNumber = nextSeqNumber;
    }

    public HashMap<Integer, Pair> getLearnCounter() {
        return learnCounter;
    }

    public void setLearnCounter(HashMap<Integer, Pair> learnCounter) {
        this.learnCounter = learnCounter;
    }

    public AtomicInteger getLocalOrderCounter() {
        return localOrderCounter;
    }

    public ArrayList<Integer> getLocalOrderList() {
        return localOrderList;
    }

    public void setLocalOrderList(ArrayList<Integer> localOrderList) {
        this.localOrderList = localOrderList;
    }

    public DadkvsServerPaxosLeader getLeader() {
        return leader;
    }

    public void setLeader(DadkvsServerPaxosLeader leader) {
        this.leader = leader;
    }

    public DadkvsServerPaxosAcceptor getAcceptor() {
        return acceptor;
    }

    public void setAcceptor(DadkvsServerPaxosAcceptor acceptor) {
        this.acceptor = acceptor;
    }

    public DadkvsServerPaxosLearner getLearner() {
        return learner;
    }

    public void setLearner(DadkvsServerPaxosLearner learner) {
        this.learner = learner;
    }

    public Map<Integer, Triplet> getPaxosLogs() {
        return paxosLogs;
    }

    public void setPaxosLogs(Map<Integer, Triplet> paxosLogs) {
        this.paxosLogs = paxosLogs;
    }

    public synchronized void updateLearnCounter(int reqid, int seqNum){
        if (!learnCounter.containsKey(reqid)) {
			learnCounter.put(reqid, new Pair(seqNum, 1));
		} else {
			// Increment LearnRequest (Num2) counter in Pair<SeqNum, counter>
            learnCounter.get(reqid).setNum2(learnCounter.get(reqid).getNum2() + 1);
			notifyAll();
		}
    }

//    public synchronized void updatePaxosLogs(int seqNum, int reqid, int priority, int config){
//        // Add log to ArrayList if it doesn't exist
//        if (!paxosLogs.containsKey(seqNum)) {
//            paxosLogs.put(seqNum, new Triplet(reqid, priority, config));
//        }
//        // Update priority if incoming priority is higher
//        if (paxosLogs.get(seqNum).getNum2() <= priority && paxosLogs.get(seqNum).getNum3() == config) {
//            if (paxosLogs.get(seqNum).getNum1() != 1){ // "-1" means not defined. reqid is only defined once in each paxosLog
//                paxosLogs.get(seqNum).setNum1(reqid);
//            }
//            paxosLogs.get(seqNum).setNum2(priority);
//        }
//    }

    public synchronized void notifyAllServerState(){
        notifyAll();
    }

    public synchronized void syncRemoveMinLocalOrder(int valueToRemove){
        if(localOrderList.contains((Integer) valueToRemove)){
            this.localOrderList.remove((Integer) valueToRemove);
        }
    }

    public synchronized void syncAddLocalOrder(int valueToAdd){
        localOrderList.add(valueToAdd);
    }
}
