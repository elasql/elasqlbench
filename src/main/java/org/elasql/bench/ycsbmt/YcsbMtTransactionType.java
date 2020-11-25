package org.elasql.bench.ycsbmt;

import org.vanilladb.bench.TransactionType;

public enum YcsbMtTransactionType implements TransactionType {
	// Loading procedures
	SCHEMA_BUILDER, TESTBED_LOADER,
	
	// Profiling
	START_PROFILING, STOP_PROFILING,
	
	// Main procedures
	YCSB_MT,
	
	// CLAY
	LAUNCH_CLAY, BROADCAST_MIGRAKEYS,

	// Migration
	START_MIGRATION, STOP_MIGRATION, MIGRATION_ANALYSIS, ASYNC_MIGRATE;
	
	public static YcsbMtTransactionType fromProcedureId(int pid) {
		return YcsbMtTransactionType.values()[pid];
	}
	
	public int getProcedureId() {
		return this.ordinal();
	}
	
	public boolean isBenchmarkingTx() {
		if (this == YCSB_MT)
			return true;
		return false;
	}
}
