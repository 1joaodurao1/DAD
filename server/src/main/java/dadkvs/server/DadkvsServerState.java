package dadkvs.server;

import java.util.*;

import dadkvs.DadkvsStep1;
import dadkvs.DadkvsStep1ServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
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
    Queue<Integer>                      orderQueue;
    final int                           n_servers;
    DadkvsStep1ServiceGrpc.DadkvsStep1ServiceStub[] async_step1_stubs;
    int nextSeqNumber;

    ManagedChannel[]    server_channels;

    boolean isFreezed;

    boolean isDelayed;



    public DadkvsServerState(int kv_size, int port, int myself, int servers,
                            DadkvsStep1ServiceGrpc.DadkvsStep1ServiceStub[] step1Stubs,
                             ManagedChannel[] channels) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        orderQueue = new LinkedList<>();
        n_servers = servers;
        async_step1_stubs = step1Stubs;
        nextSeqNumber = 0;
        main_loop = new MainLoop(this);
        main_loop_worker = new Thread (main_loop);
        main_loop_worker.start();
        isDelayed = false;
        isFreezed = false;
        server_channels = channels;
    }

    public synchronized void handleOrderID(int reqid, int seqNumber){
        while (nextSeqNumber != seqNumber){ // Force reqids to be added in order of seqNumber
            try { wait ();}
            catch (InterruptedException e) {} // Ignore
        }
        if (nextSeqNumber > seqNumber){
            System.out.println("ERROR: nextSeqNumber " + nextSeqNumber + "is HIGHER than the seqNumber received " + seqNumber);
        }
        nextSeqNumber++;
        orderQueue.add(reqid);
        notifyAll(); // To release the wait()s in handleOrderID and handleTransaction
    }

    public boolean handleTransaction(int reqid, TransactionRecord record){
        if (i_am_leader){
            DadkvsStep1.DefineOrderRequest.Builder defineOrderRequest  = DadkvsStep1.DefineOrderRequest.newBuilder();
            ArrayList<DadkvsStep1.DefineOrderReply> defineOrder_responses = new ArrayList<DadkvsStep1.DefineOrderReply>();
		    GenericResponseCollector<DadkvsStep1.DefineOrderReply> defineOrder_collector = new GenericResponseCollector<DadkvsStep1.DefineOrderReply>(defineOrder_responses, n_servers);
            defineOrderRequest.setNextReqid(reqid).setSeqNumber(nextSeqNumber++);
            for (int i = 0; i < n_servers; i++) {
                if (i != my_id){
                    CollectorStreamObserver<DadkvsStep1.DefineOrderReply> defineOrder_observer = new CollectorStreamObserver<DadkvsStep1.DefineOrderReply>(defineOrder_collector);
                    async_step1_stubs[i].defineOrder(defineOrderRequest.build(), defineOrder_observer);
                }
		    }
            defineOrder_collector.waitForTarget(n_servers - 1);  // Wait for responses from all other servers
            return this.store.commit(record);
        }

        synchronized(this){
            boolean result;
            while (orderQueue.peek() == null || reqid != orderQueue.peek()){ // Wait until you are at the front of the Queue
                try { wait ();}
                catch (InterruptedException e) {} // Ignore
            }
            result = this.store.commit(record);
            if(orderQueue.poll() == null) { // Removes item on top
                System.out.println("ERROR: transaction " + reqid + "executed OUT OF ORDER (queue was empty when it executed)");
            }
            notifyAll(); // After removing top reqid from Queue, execute next transaction immediatly if it's currently waiting
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

}
