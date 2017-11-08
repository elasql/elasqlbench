package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.server.param.ycsb.YcsbSchemaBuilderProcParamHelper;
import org.vanilladb.core.server.VanillaDb;

public class YcsbSchemaBuilderProc extends AllExecuteProcedure<YcsbSchemaBuilderProcParamHelper> {

	public YcsbSchemaBuilderProc(long txNum) {
		super(txNum, new YcsbSchemaBuilderProcParamHelper());
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
