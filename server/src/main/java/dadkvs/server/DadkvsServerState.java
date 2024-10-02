package dadkvs.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;

public class DadkvsServerState {
    boolean        i_am_leader;
    int            debug_mode;
    int            base_port;
    int            my_id;
    int            store_size;
    KeyValueStore  store;
    MainLoop       main_loop;
    Thread         main_loop_worker;
    final int                           n_servers;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;
    int nextSeqNumber;
    ManagedChannel[]    server_channels;
    boolean isFreezed;
    boolean isDelayed;
    AtomicInteger localOrder;
    HashMap<Integer, Pair> learnCounter;
    ArrayList<Integer> localOrderList;
    int minLocalorder;
    DadkvsServerPaxos paxos;



    public DadkvsServerState(int kv_size, int port, int myself, int servers,
                            DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] paxoStubs,
                             ManagedChannel[] channels) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        n_servers = servers;
        async_stubs = paxoStubs;
        nextSeqNumber = 0;
        isDelayed = false;
        isFreezed = false;
        server_channels = channels;
        localOrder = new AtomicInteger(0);
        learnCounter = new HashMap<>();
        localOrderList = new ArrayList<Integer>();
        minLocalorder = 0;
        paxos = new DadkvsServerPaxos(0,this);
        main_loop = new MainLoop(this);
        main_loop_worker = new Thread (main_loop);
        main_loop_worker.start();

    }

    public synchronized void handleOrderID(int reqid, int seqNumber){
        if (nextSeqNumber > seqNumber){
            // This request has already been processed
            System.out.println("[handleOrderID] Ignore: nextSeqNumber " + nextSeqNumber + "is HIGHER than the seqNumber received " + seqNumber);
        } else {
            if (learnCounter.containsKey(reqid)){
                // Incremente counter in Pair<SeqNum, int>
                learnCounter.get(reqid).setNum2(learnCounter.get(reqid).getNum2() + 1);
                System.out.println("[handleOrderID] Incremented HashMap entry of reqid " + reqid + "to the number" + learnCounter.get(reqid).getNum2());
            } else {
                learnCounter.put(reqid, new Pair(seqNumber, 0));
                System.out.println("[handleOrderID] Created HashMap entry of reqid " + reqid + "with the number 1");
            }
            notifyAll(); // To release the wait()s in handleTransaction to check if t
        }
    }

    public boolean handleTransaction(int reqid, TransactionRecord record){

        int localOrder_copy = this.localOrder.getAndIncrement();
        int nextSeqNumToDecide = nextSeqNumber;

        synchronized(this){

            while (isFreezed || !learnCounter.containsKey(reqid) || !(learnCounter.get(reqid).getNum1() == nextSeqNumber && learnCounter.get(reqid).getNum2() >= 2)){ 
                // Debug Messages
                System.out.println("[handleTRansaction] i_am_leader = " +  this.i_am_leader);
                System.out.println("[handleTRansaction] this.minLocalorder = " +  this.minLocalorder);
                System.out.println("[handleTRansaction] localOrder_copy = " +  localOrder_copy);
                System.out.println("[handleTRansaction] nextSeqNumToDecide = " +  nextSeqNumToDecide);
                System.out.println("[handleTRansaction] nextSeqNumber = " +  nextSeqNumber);
                if ( this.i_am_leader && this.minLocalorder == localOrder_copy && nextSeqNumToDecide == nextSeqNumber){
                    System.out.println("[handleTRansaction] Im leader and starting paxos with local order number " + localOrder_copy);
                    // chamar paxos
                    nextSeqNumToDecide = this.paxos.handleLeaderPaxos(nextSeqNumber, reqid);
                }
                else {
                    try { wait ();}
                    catch (InterruptedException e) {} // Ignore
                }
            }

            System.out.println("[handleTransaction] Im a learner and im going to commit with seqNumber " + learnCounter.get(reqid).getNum1()+
            "and request id " + reqid);
            if(localOrderList.contains(localOrder_copy)){
                this.localOrderList.remove(localOrder_copy); // in case leader is behind
                if(localOrderList.size() > 0){
                    this.minLocalorder = Collections.min(localOrderList);
                } else {
                    this.minLocalorder = 0;
                }
            }
            boolean result = this.store.commit(record);
            learnCounter.remove(reqid);
            nextSeqNumber++;
            notifyAll();
            return result;
        }
    }

    public void server_exit(){
        System.out.println("Quitting the process and exiting.");
        // desconnecting the channels before exiting
        for (int i = 0; i < n_servers; i++) {
            if (i != my_id) { // Don't make a Stub to yourself
                server_channels[i].shutdownNow();
            }
        }
        System.exit(0); // crashing the server
    }

    public void insertDelay(){
        Random random = new Random();
        int delay = random.nextInt(2500); // Random delay between 0 and 2500 ms

        // For debug purposes
        System.out.println("Random delay: " + delay + " milliseconds.");

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
                System.out.println("System shutting down");
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
                System.err.println("ERROR: Default mode not known");
                break;
        }
    }

    public void removeAndUpdateLocalOrder (){
        if(localOrderList.contains(this.minLocalorder)){
            this.localOrderList.remove(this.minLocalorder);
        }
        if(localOrderList.size() > 0){
            this.minLocalorder = Collections.min(localOrderList);
        } else {
            this.minLocalorder = 0;
        }
    }

}
