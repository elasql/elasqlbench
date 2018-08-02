package org.elasql.bench.rte.ycsb;


import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.TransactionType;

import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;
import org.vanilladb.bench.ycsb.YcsbTransactionType;
import org.vanilladb.bench.ycsb.rte.YcsbTxExecutor;

public class ElasqlYcsbRte extends RemoteTerminalEmulator {
	
	private YcsbTxExecutor executor;
	
	public ElasqlYcsbRte(SutConnection conn, StatisticMgr statMgr, int nodeId) {
		super(conn, statMgr);
		executor = new YcsbTxExecutor(new MultiTanentsParamGen(nodeId));
	}
	
	protected TransactionType getNextTxType() {
		return YcsbTransactionType.YCSB;
	}
	
	protected TransactionExecutor getTxExeutor(TransactionType type) {
		return executor;
	}
}
