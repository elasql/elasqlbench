package org.elasql.bench.server.procedure.calvin.tpce;

import org.elasql.bench.server.procedure.calvin.StartProfilingProc;
import org.elasql.bench.server.procedure.calvin.StopProfilingProc;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.vanilladb.bench.tpce.TpceTransactionType;

public class TpceStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (TpceTransactionType.fromProcedureId(pid)) {
		case SCHEMA_BUILDER:
			sp = new TpceSchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TpceTestbedLoaderProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		case TRADE_ORDER:
			sp = new TradeOrderProc(txNum);
			break;
		case TRADE_RESULT:
			sp = new TradeResultProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("Procedure " + TpceTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}

}
