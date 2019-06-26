package org.elasql.bench.rte.ycsb;


import org.elasql.bench.rte.ycsb.google.GoogleComplexWorkloadsParamGen;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;
import org.vanilladb.bench.ycsb.YcsbTransactionType;
import org.vanilladb.bench.ycsb.rte.YcsbTxExecutor;

public class ElasqlYcsbRte extends RemoteTerminalEmulator {
	
	public static final int WORKLOAD_TYPE = 1;
	
	private YcsbTxExecutor executor;
	
	public ElasqlYcsbRte(SutConnection conn, StatisticMgr statMgr, int nodeId) {
		super(conn, statMgr);
		
		if (WORKLOAD_TYPE == 1)
			// Hermes Experiments - Google Workloads
			executor = new YcsbTxExecutor(new GoogleComplexWorkloadsParamGen(nodeId));
		else if (WORKLOAD_TYPE == 2)
			// Hermes Experiments - Hotspot Workloads
			executor = new YcsbTxExecutor(new MultiTanentsParamGen(nodeId));
		
//		executor = new YcsbTxExecutor(new GoogleWorkloadsParamGen(nodeId));
//		executor = new YcsbTxExecutor(new SingleSkewWorkloadsParamGen(nodeId));
	}
	
	protected TransactionType getNextTxType() {
		return YcsbTransactionType.YCSB;
	}
	
	protected TransactionExecutor getTxExeutor(TransactionType type) {
		return executor;
	}
}
