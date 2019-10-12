package org.elasql.bench.server.procedure.tpart.tpcc;

import org.elasql.bench.server.procedure.tpart.StartProfilingProc;
import org.elasql.bench.server.procedure.tpart.StopProfilingProc;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.tpcc.TpccTransactionType;

public class TpccStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (TpccTransactionType.fromProcedureId(pid)) {
		case NEW_ORDER:
			sp = new NewOrderProc(txNum);
			break;
		case PAYMENT:
			sp = new PaymentProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		default:
			sp = null;
		}
		return sp;
	}
}
