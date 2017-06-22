package org.elasql.bench.migration;

import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.micro.ElasqlMicrobenchConstants;
import org.elasql.bench.server.metadata.MicroBenchPartitionMetaMgr;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.micro.MicroTransactionType;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class MicroMigrationManager extends MigrationManager {
	
	public static final long RECORD_PERIOD = 3000;
	private static final int COUNTS_FOR_SLEEP = 10000;
	private int parameterCounter = 0;
	
	public MicroMigrationManager() {
		super(RECORD_PERIOD);
	}
	
	@Override
	public boolean keyIsInMigrationRange(RecordKey key) {
		if (Elasql.partitionMetaMgr().isFullyReplicated(key))
			return false;
		
		int partId = MicroBenchPartitionMetaMgr.getRangeIndex(key);
		return partId == this.getDestPartition();
	}
	
	/**
	 * This should only be executed on the sequencer node.
	 */
	@Override
	public void onReceiveStartMigrationReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, MicroTransactionType.START_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call);
	}
	
	/**
	 * This should only be executed on the sequencer node.
	 * Currently not use!!
	 */
	@Override
	public void onReceiveAnalysisReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, MicroTransactionType.MIGRATION_ANALYSIS.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call);
	}
	
	/**
	 * This should only be executed on the data source node.
	 */
	@Override
	public void onReceiveAsyncMigrateReq(Object[] metadata) {
		Object[] params = getAsyncPushingParameters();
		
		if (params != null && params.length > 0)
			System.out.println("Next start key: " + params[0]);
		
		// Send a store procedure call
		Object[] call;
		if (params.length > 0) {
			call = new Object[] { new StoredProcedureCall(-1, -1, MicroTransactionType.ASYNC_MIGRATE.ordinal(), params) };
		} else
			call = new Object[] { new StoredProcedureCall(-1, -1, MicroTransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call);
	}
	
	@Override
	// XXX: Not used for now
	public void onReceiveStopMigrateReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, MicroTransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call);
	}
	
	@Override
	public void prepareAnalysis() {
		// Do nothing
	}
	
	@Override
	public Map<RecordKey, Boolean> generateDataSetForMigration() {
		// The map records the migration data set
		Map<RecordKey, Boolean> dataSet = new HashMap<RecordKey, Boolean>();
		Map<String, Constant> keyEntryMap;
		
		// Generate record keys
		// XXX: For Micro and migration
		// Hard code migrate half data
		int startId = Elasql.migrationMgr().getSourcePartition() * ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE + 1;
		int endId = startId + ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE/2-1;
		

		
		System.out.println("Migrate from ycsb_id = " + startId + " to " + endId);
		
		for (int id = startId; id <= endId; id++) {
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id",new IntegerConstant(id));
			addOrSleep(dataSet, new RecordKey("item", keyEntryMap));
		}
		
		return dataSet;
	}
	
	private void addOrSleep(Map<RecordKey, Boolean> map, RecordKey key) {
		parameterCounter++;
		
		if (parameterCounter % COUNTS_FOR_SLEEP == 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		map.put(key, Boolean.FALSE);
	}
	
	@Override
	public int recordSize(String tableName){
		switch(tableName){
		case "item":
			return 320;
		default:
			throw new IllegalArgumentException("No such table for TPCC");
		}
	}
}