package dadkvs.server.Paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

import dadkvs.server.*;


class DadkvsServerPaxos {

	int my_current_config;
	int my_current_priority;
	DadkvsServerState server_state;
	public final int numPaxosServers = 3;


	public DadkvsServerPaxos(int config, DadkvsServerState state) {
		my_current_priority = state.getMy_id();
		my_current_config = config;
		server_state = state;
	}

	public int getMy_current_config() {
		return my_current_config;
	}
	public void setMy_current_config(int my_current_config) {
		this.my_current_config = my_current_config;
	}

}
