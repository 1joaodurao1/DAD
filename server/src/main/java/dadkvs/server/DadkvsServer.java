package dadkvs.server;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;

import dadkvs.DadkvsStep1ServiceGrpc;

public class DadkvsServer {

    static DadkvsServerState server_state;

    /** Server host port. */
    private static int port;
	private final static int n_servers = 5;

    public static void main(String[] args) throws Exception {
		final int kvsize = 1000;

		System.out.println(DadkvsServer.class.getSimpleName());

		// Print received arguments.
		System.out.printf("Received %d arguments%n", args.length);
		for (int i = 0; i < args.length; i++) {
			System.out.printf("arg[%d] = %s%n", i, args[i]);
		}

		// Check arguments.
		if (args.length < 2) {
			System.err.println("Argument(s) missing!");
			System.err.printf("Usage: java %s baseport replica-id%n", Server.class.getName());
			return;
		}

		int base_port = Integer.valueOf(args[0]);
		int my_id     = Integer.valueOf(args[1]);

		String host = "localhost";
		String[] targets  = new String[n_servers];
		for (int i = 0; i < n_servers; i++) { // Create a target address for each server
			int target_port = base_port + i;
			targets[i] = new String();
			targets[i] = host + ":" + target_port;
			System.out.printf("targets[%d] = %s%n", i, targets[i]);
		}

		ManagedChannel[] channels = new ManagedChannel[n_servers];
		for (int i = 0; i < n_servers; i++) {
			if (i != my_id){ // Don't make a channel to yourself
				channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
			}
		}
		DadkvsStep1ServiceGrpc.DadkvsStep1ServiceStub[] step1_stubs = new DadkvsStep1ServiceGrpc.DadkvsStep1ServiceStub[n_servers];
		for (int i = 0; i < n_servers; i++) {
			if (i != my_id){ // Don't make a Stub to yourself
				step1_stubs[i] = DadkvsStep1ServiceGrpc.newStub(channels[i]);
			}
		}

		server_state = new DadkvsServerState(kvsize, base_port, my_id, n_servers, step1_stubs, channels); // Creating this State Machine starts the Main Loop

		port = base_port + my_id;

		final BindableService service_impl = new DadkvsMainServiceImpl(server_state);
		final BindableService console_impl = new DadkvsConsoleServiceImpl(server_state);
		final BindableService paxos_impl   = new DadkvsPaxosServiceImpl(server_state);
		final BindableService step1_impl   = new DadkvsStep1ServiceImpl(server_state);

		// Create a new server to listen on port.
		Server server = ServerBuilder.forPort(port).addService(service_impl).addService(console_impl).addService(paxos_impl).addService(step1_impl).build();
		// Start the server.
		server.start();
		// Server threads are running in the background.
		System.out.println("Server started");

		// Do not exit the main thread. Wait until server is terminated.
		server.awaitTermination();
    }
}
