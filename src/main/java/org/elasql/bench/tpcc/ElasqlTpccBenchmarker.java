package org.elasql.bench.tpcc;

import org.elasql.bench.rte.tpcc.ElasqlTpccRte;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.tpcc.TpccBenchmarker;
import org.vanilladb.bench.tpcc.TpccConstants;

public class ElasqlTpccBenchmarker extends TpccBenchmarker {
	
	private int nextWid = 0;
	
	public ElasqlTpccBenchmarker(SutDriver sutDriver) {
		super(sutDriver);
	}
	
	@Override
	protected RemoteTerminalEmulator createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of TpccRte here
		RemoteTerminalEmulator rte = new ElasqlTpccRte(conn, statMgr, nextWid + 1);
		nextWid = (nextWid + 1) % TpccConstants.NUM_WAREHOUSES;
		return rte;
	}
}
