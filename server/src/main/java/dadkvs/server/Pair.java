package dadkvs.server;

public class Pair {

    private int sequenceNumber;
    private int requestCounter;


    public Pair(int seqNumber , int reqCounter){

        sequenceNumber = seqNumber;
        requestCounter = reqCounter;
    }

    public int getNum1(){return this.sequenceNumber;}

    public int getNum2(){return this.requestCounter;}

    public void setNum1( int seqNumber){ this.sequenceNumber = seqNumber; }

    public void setNum2 (int reqCounter) { this.requestCounter = reqCounter;}
}
