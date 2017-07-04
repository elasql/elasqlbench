package org.elasql.bench.server.procedure.calvin.tpce;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.server.param.tpce.TpceSchemaBuilderParamHelper;
import org.vanilladb.core.server.VanillaDb;

public class TpceSchemaBuilderProc extends AllExecuteProcedure<TpceSchemaBuilderParamHelper> {
	private static Logger logger = Logger.getLogger(TpceSchemaBuilderProc.class.getName());
	
	public TpceSchemaBuilderProc(long txNum) {
		super(txNum, new TpceSchemaBuilderParamHelper());
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
		// XXX: We should lock those tables
		// localWriteTables.addAll(Arrays.asList(paramHelper.getTableNames()));
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create schema for tpce testbed...");
		
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);

	}
}
