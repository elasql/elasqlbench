package org.elasql.bench.micro;

import org.elasql.bench.rte.micro.ElasqlMicrobenchRte;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.tpcc.TpccBenchmarker;

public class ElasqlMicroBenchmarker extends TpccBenchmarker {
	
	// XXX: Add report postfix
	public ElasqlMicroBenchmarker(SutDriver sutDriver) {
		super(sutDriver);
	}
	
	@Override
	protected RemoteTerminalEmulator createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of MicroRte here
		RemoteTerminalEmulator rte = new ElasqlMicrobenchRte(conn, statMgr);
		return rte;
	}
}
