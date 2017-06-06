package org.elasql.bench.server.procedure.calvin;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class StopProfilingProc extends AllExecuteProcedure<StoredProcedureParamHelper> {

	public StopProfilingProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}

	@Override
	protected void prepareKeys() {
		// do nothing

	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		Elasql.stopProfilerAndReport();

	}

}
