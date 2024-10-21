package dadkvs.server;

public class Triplet {

    private int num1; // reqid
    private int num2; // priority
    private int num3; // config


    public Triplet(int num1 , int num2, int num3){

        this.num1 = num1;
        this.num2 = num2;
        this.num3 = num3;
    }

    public int getNum1(){return this.num1;}

    public int getNum2(){return this.num2;}

    public void setNum1( int num1){ this.num1 = num1; }

    public void setNum2 (int num2) { this.num2 = num2;}

    public int getNum3(){return this.num3;}

    public void setNum3( int num3){ this.num3 = num3; }
}
