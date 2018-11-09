package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.benchmarks.tpcc.rte.ElasqlTpccRte;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class TpccStandardRteGenerator implements TpccRteGenerator {
	
	private int nextRteId = 0;
	
	@Override
	public int getNumOfRTEs() {
		return BenchmarkerParameters.NUM_RTES;
	}

	@Override
	public RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of TpccRte here
		ElasqlTpccRte rte = new ElasqlTpccRte(conn, statMgr, nextRteId / 10 + 1, nextRteId % 10 + 1);
		nextRteId++;
		return rte;
	}
}
