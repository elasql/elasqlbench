package org.elasql.bench.benchmarks.recon;

import org.vanilladb.bench.BenchTransactionType;

public enum ReconbenchTransactionType implements BenchTransactionType {
	// Loading procedures
	TESTBED_LOADER(false),

	// Database checking procedures
	CHECK_DATABASE(false),

	// Benchmarking procedures
	RECON(true), EXECUTE(true), UPDATE(true);

	public static ReconbenchTransactionType fromProcedureId(int pid) {
		return ReconbenchTransactionType.values()[pid];
	}

	private boolean isBenchProc;

	ReconbenchTransactionType(boolean isBenchProc) {
		this.isBenchProc = isBenchProc;
	}

	@Override
	public int getProcedureId() {
		return this.ordinal();
	}

	@Override
	public boolean isBenchmarkingProcedure() {
		return isBenchProc;
	}
}
