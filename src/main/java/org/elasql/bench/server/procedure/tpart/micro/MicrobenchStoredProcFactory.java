package org.elasql.bench.server.procedure.tpart.micro;



import org.elasql.bench.server.procedure.tpart.StartProfilingProc;
import org.elasql.bench.server.procedure.tpart.StopProfilingProc;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.micro.MicroTransactionType;

public class MicrobenchStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (MicroTransactionType.fromProcedureId(pid)) {
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
