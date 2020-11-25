package org.elasql.bench.server.procedure.calvin.ycsbmt;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.ycsbmt.YcsbMtConstants;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class YcsbMtTestbedLoaderProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(YcsbMtTestbedLoaderProc.class.getName());
	
	private int loadedCount = 0;
	
	public YcsbMtTestbedLoaderProc(long txNum) {
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
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(false);
		
		// Note: we do not consider scaling-out or consolidation case
		// so we generate data for every partition
		int tableId = Elasql.serverId();
		generateRecords(tableId, 1, YcsbMtConstants.INIT_RECORDS_PER_PART);

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		// Delete the log file and create a new one
		VanillaDb.logMgr().removeAndCreateNewLog();

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished. " + loadedCount + " YCSB records are loaded.");

	}
	
	private void generateRecords(int tableId, int startId, int recordCount) {
		int endId = startId + recordCount - 1;
		
		// Check field count
		int fieldCount = YcsbMtConstants.FIELD_COUNT_IF_FIXED;
		if (YcsbMtConstants.IS_DYNAMIC_FIELD_COUNT) {
			// add a column per 2 tables
			fieldCount = (tableId + 4) / 2;
		}
		
		// Names
		String tableName = String.format("ycsb_%d", tableId);
		String keyName = String.format("ycsb_%d_id", tableId);
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Start populating %s table from id = %d to id = %d (count = %d)",
					tableName, startId, endId, recordCount));
		
		// Generate the field names of YCSB table
		String sqlHead = generateInsertSqlHead(tableName, keyName, fieldCount);
		
		StringBuilder sql;
		String ycsbId, ycsbValue;
		RecordKey key;
		for (int id = startId; id <= endId; id++) {
			// The primary key of YCSB is the string format of id
			ycsbId = String.format(YcsbMtConstants.ID_FORMAT, id);
			
			// Check if it is a local record
			key = new RecordKey(tableName, keyName, new VarcharConstant(ycsbId));
			if (Elasql.partitionMetaMgr().getPartition(key) == Elasql.serverId()) {
				
				// YCSB ID
				sql = new StringBuilder(sqlHead);
				sql.append("'");
				sql.append(ycsbId);
				sql.append("'");
				
				// All values of the fields use the same value
				ycsbValue = ycsbId;
				
				for (int fldId = 1; fldId < fieldCount; fldId++) {
					sql.append(", '");
					sql.append(ycsbValue);
					sql.append("'");
				}
				sql.append(")");
				
				int result = VanillaDb.newPlanner().executeUpdate(sql.toString(), tx);
				if (result <= 0)
					throw new RuntimeException();
				
				loadedCount++;
				if (loadedCount % 50000 == 0)
					if (logger.isLoggable(Level.INFO))
						logger.info(String.format("%d %s records have been populated.",
								loadedCount, tableName));
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Populating YCSB table completed.");
	}
	
	private String generateInsertSqlHead(String tableName, String keyName, int fieldCount) {
		StringBuilder sb = new StringBuilder();
		
		// table name + primary key
		sb.append(String.format("INSERT INTO %s (%s", tableName, keyName));
		
		// data fields
		for (int fldId = 1; fldId < fieldCount; fldId++)
			sb.append(String.format(", %s_%d", tableName, fldId));
		
		// Ending
		sb.append(") VALUES (");
		
		return sb.toString();
	}
}
