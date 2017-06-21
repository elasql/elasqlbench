package org.elasql.bench.rte.micro;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.micro.MicroTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;
import org.vanilladb.bench.rte.micro.MicrobenchmarkTxExecutor;

public class ElasqlMicrobenchRte extends RemoteTerminalEmulator {
	
	private MicrobenchmarkTxExecutor executor;

	public ElasqlMicrobenchRte(SutConnection conn, StatisticMgr statMgr) {
		super(conn, statMgr);
		executor = new MicrobenchmarkTxExecutor(new ElasqlMicroRealisticbenchmarkParamGen());
	}
	
	protected TransactionType getNextTxType() {
		return MicroTransactionType.MICRO;
	}
	
	protected TransactionExecutor getTxExeutor(TransactionType type) {
		return executor;
	}
}
