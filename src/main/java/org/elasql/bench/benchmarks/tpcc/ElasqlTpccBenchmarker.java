package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.benchmarks.tpcc.rte.ElasqlTpccRte;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccBenchmarker;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ElasqlTpccBenchmarker extends TpccBenchmarker {
	
	private int nextWid = 0;
	
	public ElasqlTpccBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver, Integer.toString(nodeId));
	}
	
	@Override
	protected RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of TpccRte here
		ElasqlTpccRte rte = new ElasqlTpccRte(conn, statMgr, nextWid / 10 + 1, nextWid % 10 + 1);
		nextWid = (nextWid + 1) % TpccConstants.NUM_WAREHOUSES;
		return rte;
	}
}
