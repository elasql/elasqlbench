package org.elasql.bench.server.procedure.tpart.ycsb;

import org.elasql.bench.server.procedure.tpart.ycsb.YcsbBenchmarkProc;
import org.elasql.bench.server.procedure.tpart.StartProfilingProc;
import org.elasql.bench.server.procedure.tpart.StopProfilingProc;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.micro.MicroTransactionType;
import org.vanilladb.bench.ycsb.YcsbTransactionType;

public class YcsbStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (YcsbTransactionType.fromProcedureId(pid)) {
			case START_PROFILING:
				sp = new StartProfilingProc(txNum);
				break;
			case STOP_PROFILING:
				sp = new StopProfilingProc(txNum);
				break;
			case YCSB:
				sp = new YcsbBenchmarkProc(txNum);
				break;
			default:
				throw new UnsupportedOperationException("Procedure " + MicroTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
