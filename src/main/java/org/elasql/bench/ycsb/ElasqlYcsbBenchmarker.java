package org.elasql.bench.ycsb;

import org.elasql.bench.rte.ycsb.ElasqlYcsbRte;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.tpcc.TpccBenchmarker;

public class ElasqlYcsbBenchmarker extends TpccBenchmarker {
	private int nodeId;
	
	public ElasqlYcsbBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver);
		this.nodeId = nodeId;
	}
	
	@Override
	protected RemoteTerminalEmulator createRte(SutConnection conn, StatisticMgr statMgr) {
		RemoteTerminalEmulator rte = new ElasqlYcsbRte(conn, statMgr, nodeId);
		return rte;
	}
}
