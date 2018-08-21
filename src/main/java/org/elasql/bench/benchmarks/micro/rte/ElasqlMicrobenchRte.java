package org.elasql.bench.benchmarks.micro.rte;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.micro.MicrobenchmarkTxnType;
import org.vanilladb.bench.benchmarks.micro.rte.MicrobenchmarkTxExecutor;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;

public class ElasqlMicrobenchRte extends RemoteTerminalEmulator<MicrobenchmarkTxnType> {
	
	private MicrobenchmarkTxExecutor executor;

	public ElasqlMicrobenchRte(SutConnection conn, StatisticMgr statMgr) {
		super(conn, statMgr);
		executor = new MicrobenchmarkTxExecutor(new ElasqlMicrobenchParamGen());
	}
	
	protected MicrobenchmarkTxnType getNextTxType() {
		return MicrobenchmarkTxnType.MICRO_TXN;
	}

	@Override
	protected TransactionExecutor<MicrobenchmarkTxnType> getTxExeutor(MicrobenchmarkTxnType type) {
		return executor;
	}
}
