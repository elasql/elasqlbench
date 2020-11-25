package org.elasql.bench.ycsbmt;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.elasql.bench.rte.ycsbmt.YcsbMtRte;
import org.vanilladb.bench.Benchmarker;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class YcsbMtBenchmarker extends Benchmarker {

	private int nodeId;
	
	public YcsbMtBenchmarker(SutDriver sutDriver) {
		super(sutDriver);
	}
	
	public YcsbMtBenchmarker(SutDriver sutDriver, String reportPostfix) {
		super(sutDriver, "ycsb-mt-" + reportPostfix);
	}
	
	public YcsbMtBenchmarker(SutDriver sutDriver, int nodeId) {
		super(sutDriver, "" + nodeId);
		this.nodeId = nodeId;
	}
	
	public Set<TransactionType> getBenchmarkingTxTypes() {
		Set<TransactionType> txTypes = new HashSet<TransactionType>();
		for (TransactionType txType : YcsbMtTransactionType.values()) {
			if (txType.isBenchmarkingTx())
				txTypes.add(txType);
		}
		return txTypes;
	}
	
	protected void executeLoadingProcedure(SutConnection conn) throws SQLException {
		conn.callStoredProc(YcsbMtTransactionType.SCHEMA_BUILDER.ordinal());
		conn.callStoredProc(YcsbMtTransactionType.TESTBED_LOADER.ordinal());
	}
	
	protected RemoteTerminalEmulator createRte(SutConnection conn, StatisticMgr statMgr) {
		RemoteTerminalEmulator rte = new YcsbMtRte(conn, statMgr, nodeId);
		return rte;
	}
	
	protected void startProfilingProcedure(SutConnection conn) throws SQLException {
		conn.callStoredProc(YcsbMtTransactionType.START_PROFILING.ordinal());
	}
	
	protected void stopProfilingProcedure(SutConnection conn) throws SQLException {
		conn.callStoredProc(YcsbMtTransactionType.STOP_PROFILING.ordinal());
	}
}
