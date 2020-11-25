package org.elasql.bench.server.procedure.calvin.ycsbmt;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.ycsbmt.YcsbMtConstants;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class YcsbMtSchemaBuilderProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(YcsbMtSchemaBuilderProc.class.getName());

	public YcsbMtSchemaBuilderProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}
	
	@Override
	protected void prepareKeys() {
		// Note: we do not lock the tables because there should not be
		// any other concurrent transactions during loading testbed.
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Starting creating tables and indices for YCSB_MT...");
		
		// Each partition must create every table in its database
		// in order to reserve for record migration
		for (int tableId = 0; tableId < PartitionMetaMgr.NUM_PARTITIONS; tableId++) {
			String sql = generateYcsbTableDdl(tableId);

			if (logger.isLoggable(Level.FINE))
				logger.fine("Applying: " + sql);
			
			VanillaDb.newPlanner().executeUpdate(sql, tx);
			
			sql = generateYcsbIndexDdl(tableId);

			if (logger.isLoggable(Level.FINE))
				logger.fine("Applying: " + sql);
			
			VanillaDb.newPlanner().executeUpdate(sql, tx);
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Finished creating tables and indices for YCSB_MT.");
	}
	
	private String generateYcsbTableDdl(int tableId) {
		StringBuilder sb = new StringBuilder();
		
		// table name + primary key
		sb.append(String.format("CREATE TABLE ycsb_%d ( ycsb_%d_id VARCHAR(%d)",
				tableId, tableId, YcsbMtConstants.CHARS_PER_FIELD));
		
		// data fields
		int fieldCount = YcsbMtConstants.FIELD_COUNT_IF_FIXED;
		if (YcsbMtConstants.IS_DYNAMIC_FIELD_COUNT)
			// add a column per 2 tables
			fieldCount = (tableId + 4) / 2;
		for (int fldId = 1; fldId < fieldCount; fldId++) {
			sb.append(String.format(", ycsb_%d_%d VARCHAR(%d)",
					tableId, fldId, YcsbMtConstants.CHARS_PER_FIELD));
		}
		
		// Ending
		sb.append(")");
		
		return sb.toString();
	}
	
	private String generateYcsbIndexDdl(int tableId) {
		return String.format("CREATE INDEX idx_ycsb_%d ON ycsb_%d (ycsb_%d_id)",
				tableId, tableId, tableId);
	}
}
