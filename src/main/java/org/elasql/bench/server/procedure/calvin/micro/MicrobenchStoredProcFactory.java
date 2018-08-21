package org.elasql.bench.server.procedure.calvin.micro;

import org.elasql.bench.server.procedure.calvin.StartProfilingProc;
import org.elasql.bench.server.procedure.calvin.StopProfilingProc;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.vanilladb.bench.benchmarks.micro.MicrobenchmarkTxnType;

public class MicrobenchStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (MicrobenchmarkTxnType.fromProcedureId(pid)) {
		case TESTBED_LOADER:
			sp = new MicroTestbedLoaderProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		case MICRO_TXN:
			sp = new MicroTxnProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("Procedure " + MicrobenchmarkTxnType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
