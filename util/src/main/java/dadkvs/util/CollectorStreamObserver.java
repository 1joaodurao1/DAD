package dadkvs.util;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class CollectorStreamObserver<T> implements StreamObserver<T> {

    dadkvs.util.GenericResponseCollector collector;
    boolean done;

    public CollectorStreamObserver (GenericResponseCollector c) {
        collector = c;
	done = false;
    }

    @Override
    public void onNext(T value) {
        // Handle the received response of type T
        //System.out.println("Received response: " + value);
	if (done == false) {
	    collector.addResponse(value);
	    done = true;
	}
    }

    @Override
    public void onError(Throwable t) {
        // Handle error
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException ex = (StatusRuntimeException) t;
            Status.Code code = ex.getStatus().getCode();  // gRPC status code
            if (code == Status.Code.UNAVAILABLE) {
                System.out.println("Server dead or very slow network");
            } else {
                System.out.println("ERROR:" + t.getMessage());
            }
        }
        if (done == false) {
            collector.addNoResponse();
            done = true;
        }
    }

    @Override
    public void onCompleted() {
        // Handle stream completion
        //System.out.println("Stream completed");
	if (done == false) {
	    collector.addNoResponse();
	    done = true;
	}
    }
}
