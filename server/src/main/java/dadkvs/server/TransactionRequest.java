package dadkvs.server;

public class TransactionRequest {

    private boolean isRead;

    private int read_key;
    private TransactionRecord record;


    public TransactionRequest(int key) {
        this.read_key = key1;
        this.isRead= true;
        this.record = null;
    }

    public TransactionRequest(TransactionRecord transaction ) {
        this.isRead = false;
        this.record = transaction;
        this.read_key = -1;
    }

    // Getter and Setter methods for all fields
    public void setisRead ( boolean isRead) { this.isRead = isRead;}

    public boolean getisRead (){ return this.isRead;}

    public void setRecord( TransactionRecord record) {this.record = record;}

    public TransactionRecord getRecord(){ return this.record;}

    public void setReadKey(int key){ this.read_key = key;}

    public int getReadKey(){ return this.read_key;}

}
