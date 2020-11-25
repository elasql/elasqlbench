package org.elasql.bench.rte.ycsbmt;

import org.elasql.bench.ycsbmt.YcsbMtTransactionType;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;

public class YcsbMtRte extends RemoteTerminalEmulator {
	
	private YcsbMtTxExecutor executor;
	
	public YcsbMtRte(SutConnection conn, StatisticMgr statMgr, int nodeId) {
		super(conn, statMgr);
		executor = new YcsbMtTxExecutor(new YcsbMtGoogleWorkloadsParamGen());
	}
	
	protected TransactionType getNextTxType() {
		return YcsbMtTransactionType.YCSB_MT;
	}
	
	protected TransactionExecutor getTxExeutor(TransactionType type) {
		return executor;
	}
}

