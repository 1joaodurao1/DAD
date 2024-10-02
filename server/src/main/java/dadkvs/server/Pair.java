package dadkvs.server;

public class Pair {

    private int sequenceNumber;
    private int requestCounter;


    public Pair(int seqNumber , int reqCounter){

        sequenceNumber = seqNumber;
        requestCounter = reqCounter;
    }

    public int getSeqNumber(){return this.sequenceNumber;}

    public int getReqCounter(){return this.requestCounter;}

    public void setSeqNumber( int seqNumber){ this.sequenceNumber = seqNumber; }

    public void setReqCounter (int reqCounter) { this.requestCounter = reqCounter;}

    
}
