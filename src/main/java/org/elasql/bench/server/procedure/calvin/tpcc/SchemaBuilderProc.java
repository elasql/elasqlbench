package org.elasql.bench.server.procedure.calvin.tpcc;

import java.util.Map;

import org.elasql.bench.server.param.tpcc.SchemaBuilderProcParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.server.VanillaDb;

public class SchemaBuilderProc extends AllExecuteProcedure<SchemaBuilderProcParamHelper> {

	public SchemaBuilderProc(long txNum) {
		super(txNum, new SchemaBuilderProcParamHelper());
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
		// XXX: We should lock those tables
		// localWriteTables.addAll(Arrays.asList(paramHelper.getTableNames()));
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);

	}
}