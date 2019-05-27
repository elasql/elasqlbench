package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class YcsbTestbedLoaderProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(YcsbTestbedLoaderProc.class.getName());
	
	private int loadedCount = 0;
	
	public YcsbTestbedLoaderProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}

	@Override
	protected void prepareKeys() {
		// do nothing
		// XXX: We should lock those tables
		// List<String> writeTables = Arrays.asList(paramHelper.getTables());
		// localWriteTables.addAll(writeTables);

	}
	
	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(false);
		
		// Generate item records
//		int startIId = Elasql.serverId() * ElasqlYcsbConstants.MAX_RECORD_PER_PART + 1;
//		generateItems(startIId, ElasqlYcsbConstants.RECORD_PER_PART);
		int numOfParts = Elasql.partitionMetaMgr().getCurrentNumOfParts();
		generateItems(1, ElasqlYcsbConstants.RECORD_PER_PART * numOfParts);

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
	
	private void generateItems(int startId, int recordCount) {
		// For scaling-out experiments
//		if (Elasql.serverId() > 2)
//			return;
		
		int endId = startId + recordCount - 1;
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Start populating YCSB table from i_id=" + startId
					+ " to i_id=" + endId + " (count = " + recordCount + ")");
		
		// Generate the field names of YCSB table
		String sqlPrefix = "INSERT INTO ycsb (ycsb_id";
		for (int count = 1; count < YcsbConstants.FIELD_COUNT; count++) {
			sqlPrefix += ", ycsb_" + count;
		}
		sqlPrefix += ") VALUES (";
		
		String sql;
		String ycsbId, ycsbValue;
		RecordKey key;
		for (int id = startId; id <= endId; id++) {
			
			// The primary key of YCSB is the string format of id
			ycsbId = String.format(YcsbConstants.ID_FORMAT, id);
			
			// Check if it is a local record
			key = new RecordKey("ycsb", "ycsb_id", new VarcharConstant(ycsbId));
			if (Elasql.partitionMetaMgr().getPartition(key) == Elasql.serverId()) {
			
				sql = sqlPrefix + "'" + ycsbId + "'";
				
				// All values of the fields use the same value
				ycsbValue = ycsbId;
				
				for (int count = 1; count < YcsbConstants.FIELD_COUNT; count++) {
					sql += ", '" + ycsbValue + "'";
				}
				sql += ")";
	
				int result = VanillaDb.newPlanner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
				
				loadedCount++;
				if (loadedCount % 50000 == 0)
					if (logger.isLoggable(Level.INFO))
						logger.info(loadedCount + " YCSB records has been populated.");
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Populating YCSB table completed.");
	}
}
