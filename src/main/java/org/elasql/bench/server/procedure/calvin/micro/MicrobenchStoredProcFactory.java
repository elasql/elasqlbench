package org.elasql.bench.server.procedure.calvin.micro;

import org.elasql.bench.server.procedure.calvin.StartProfilingProc;
import org.elasql.bench.server.procedure.calvin.StopProfilingProc;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
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
		default:
			throw new UnsupportedOperationException("Procedure " + MicroTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
