package org.elasql.bench.server.procedure.calvin.micro;

import org.elasql.bench.server.procedure.calvin.StartProfilingProc;
import org.elasql.bench.server.procedure.calvin.StopProfilingProc;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.elasql.server.migration.procedure.AsyncMigrateProc;
import org.elasql.server.migration.procedure.MigrationAnalysisProc;
import org.elasql.server.migration.procedure.StartMigrationProc;
import org.elasql.server.migration.procedure.StopMigrationProc;
import org.vanilladb.bench.micro.MicroTransactionType;

public class MicrobenchStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (MicroTransactionType.fromProcedureId(pid)) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		case MICRO:
			sp = new MicroBenchmarkProc(txNum);
			break;
		case START_MIGRATION:
			sp = new StartMigrationProc(txNum);
			break;
		case STOP_MIGRATION:
			sp = new StopMigrationProc(txNum);
			break;
		case ASYNC_MIGRATE:
			sp = new AsyncMigrateProc(txNum);
			break;
		case MIGRATION_ANALYSIS:
			sp = new MigrationAnalysisProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("Procedure " + MicroTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
