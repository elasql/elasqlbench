package org.elasql.bench.server.procedure.calvin.ycsbmt;

import org.elasql.bench.server.procedure.calvin.StartProfilingProc;
import org.elasql.bench.server.procedure.calvin.StopProfilingProc;
import org.elasql.bench.ycsbmt.YcsbMtTransactionType;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;

public class YcsbMtStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (YcsbMtTransactionType.fromProcedureId(pid)) {
			case SCHEMA_BUILDER:
				sp = new YcsbMtSchemaBuilderProc(txNum);
				break;
			case TESTBED_LOADER:
				sp = new YcsbMtTestbedLoaderProc(txNum);
				break;
			case START_PROFILING:
				sp = new StartProfilingProc(txNum);
				break;
			case STOP_PROFILING:
				sp = new StopProfilingProc(txNum);
				break;
			case YCSB_MT:
				sp = new YcsbMtBenchmarkProc(txNum);
				break;
			default:
				throw new UnsupportedOperationException("Procedure " + YcsbMtTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
