package org.elasql.bench.server.procedure.calvin.ycsb;

import org.elasql.bench.server.procedure.calvin.StartProfilingProc;
import org.elasql.bench.server.procedure.calvin.StopProfilingProc;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.ycsb.YcsbTransactionType;

public class YcsbStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (YcsbTransactionType.fromProcedureId(pid)) {
			case SCHEMA_BUILDER:
				sp = new YcsbSchemaBuilderProc(txNum);
				break;
			case TESTBED_LOADER:
				if (PartitionMetaMgr.LOAD_METIS_PARTITIONS)
					sp = new SchismYcsbTestbedLoader(txNum);
				else
					sp = new YcsbTestbedLoaderProc(txNum);
				break;
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
				throw new UnsupportedOperationException("Procedure " + YcsbTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
