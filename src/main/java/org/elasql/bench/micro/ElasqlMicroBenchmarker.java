package org.elasql.bench.micro;

import java.util.logging.Level;

import org.elasql.bench.rte.micro.ElasqlMicrobenchRte;
import org.elasql.bench.App;
import org.vanilladb.bench.Benchmarker;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.micro.MicroTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.tpcc.TpccBenchmarker;

public class ElasqlMicroBenchmarker extends TpccBenchmarker {

	public ElasqlMicroBenchmarker(SutDriver sutDriver) {
		super(sutDriver);
	}

	@Override
	protected RemoteTerminalEmulator createRte(SutConnection conn, StatisticMgr statMgr) {
		// NOTE: We use a customized version of MicroRte here
		RemoteTerminalEmulator rte = new ElasqlMicrobenchRte(conn, statMgr);
		return rte;
	}

	@Override
	public void startMigration() {
		if ( App.getNodeId() == 0) {
			if (Benchmarker.getLogger().isLoggable(Level.INFO))
				Benchmarker.getLogger().info("start migration at: " + System.currentTimeMillis());
			try {
				SutConnection spc = Benchmarker.getConnection();
				spc.callStoredProc(MicroTransactionType.MIGRATION_ANALYSIS.ordinal());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
