package org.elasql.bench.server.procedure.tpart.ycsbmt;

import org.elasql.bench.server.procedure.tpart.StartProfilingProc;
import org.elasql.bench.server.procedure.tpart.StopProfilingProc;
import org.elasql.bench.ycsbmt.YcsbMtTransactionType;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.micro.MicroTransactionType;

public class YcsbMtStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (YcsbMtTransactionType.fromProcedureId(pid)) {
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
				throw new UnsupportedOperationException("Procedure " + MicroTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
