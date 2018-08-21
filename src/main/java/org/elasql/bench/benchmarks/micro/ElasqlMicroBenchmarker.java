package org.elasql.bench.benchmarks.micro;

import org.elasql.bench.benchmarks.micro.rte.ElasqlMicrobenchRte;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.micro.MicroBenchmarker;
import org.vanilladb.bench.benchmarks.micro.MicrobenchmarkTxnType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ElasqlMicroBenchmarker extends MicroBenchmarker {
	
	public ElasqlMicroBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver, Integer.toString(nodeId));
	}
	
	@Override
	protected RemoteTerminalEmulator<MicrobenchmarkTxnType> createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of MicroRte here
		return new ElasqlMicrobenchRte(conn, statMgr);
	}
}
