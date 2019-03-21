package org.elasql.bench.migration;

import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.rte.ycsb.GoogleWorkloadsParamGen;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.micro.MicroTransactionType;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class YcsbMigrationManager extends MigrationManager {

	public static final long RECORD_PERIOD = 3000;
	private static final int COUNTS_FOR_SLEEP = 10000;
	private int parameterCounter = 0;
	
//	public static final int VERTEX_PER_PART = ElasqlYcsbConstants.RECORD_PER_PART / DATA_RANGE_SIZE;

	public YcsbMigrationManager(int nodeId) {
		super(RECORD_PERIOD, nodeId);
	}
	
	@Override
	public int getRecordCount() {
		return PartitionMetaMgr.NUM_PARTITIONS * ElasqlYcsbConstants.RECORD_PER_PART;
	}
	
	@Override
	public int retrieveIdAsInt(RecordKey k) {
		return Integer.parseInt(k.getKeyVal("ycsb_id").toString());
	}
	
	@Override
	public long getWaitingTime() {
//		return 1000 * 1000 * 1000; // very long time
		// 30 seconds for the delay of starting sending transactions
		return GoogleWorkloadsParamGen.WARMUP_TIME + 30 * 1000;
//		return SingleSkewWorkloadsParamGen.WARMUP_TIME;
//		return MultiTanentsParamGen.WARMUP_TIME;
//		return 120 * 1000; // for scaling-out & consolidation
	}
	
	@Override
	public long getMigrationPreiod() {
		// For Google workloads
		if (PartitionMetaMgr.USE_SCHISM)
			return GoogleWorkloadsParamGen.REPLAY_PREIOD - MONITORING_TIME;
		else
			return 100 * 1000;
//		return SingleSkewWorkloadsParamGen.CHANGING_PERIOD;
//		return MultiTanentsParamGen.CHANGING_PERIOD;
//		return 1000 * 1000; // for scaling-out & consolidation
	}
	
	public long getMigrationStopTime() {
		return GoogleWorkloadsParamGen.REPLAY_PREIOD;
//		return 400 * 1000;
//		return 1000 * 1000; // only stop after long time (scaling-out & consolidation, multitanent)
	}

	/**
	 * This should only be executed on the sequencer node. Currently not use!!
	 */
	@Override
	public void onReceiveAnalysisReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = {
				new StoredProcedureCall(-1, -1, MicroTransactionType.MIGRATION_ANALYSIS.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}

	/**
	 * This should only be executed on the sequencer node.
	 */
	@Override
	public void onReceiveStartMigrationReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = {
				new StoredProcedureCall(-1, -1, MicroTransactionType.START_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, true);
	}

	/**
	 * This should only be executed on the data source node.
	 */
	@Override
	public void onReceiveAsyncMigrateReq(Object[] metadata) {

		System.out.println("Revoive Async at source");
		Object[] params = getAsyncPushingParameters();

		if (params != null && params.length > 0)
			System.out.println("Next start key: " + params[0]);

		// Send a store procedure call
		Object[] call;
		if (params.length > 0) {
			call = new Object[] {
					new StoredProcedureCall(-1, -1, MicroTransactionType.ASYNC_MIGRATE.ordinal(), params) };
		} else
			call = new Object[] {
					new StoredProcedureCall(-1, -1, MicroTransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, true);
	}

	/**
	 * This should only be executed on the sequencer node.
	 */

	@Override
	public void sendLaunchClayReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1,
				MicroTransactionType.LAUNCH_CLAY.ordinal(), (Object[]) null) };
		// Cannot be called from Appia thread
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}

	/**
	 * This should only be executed on the sequencer node.
	 */
	@Override
	public void broadcastMigrateKeys(Object[] params) {
		Object[] call;
		call = new Object[] { new StoredProcedureCall(-1, -1,
				MicroTransactionType.BROADCAST_MIGRAKEYS.ordinal(), params) };
		// Cannot be called from Appia thread
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}

	@Override
	// XXX: Not used for now
	public void onReceiveStopMigrateReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = {
				new StoredProcedureCall(-1, -1, MicroTransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, true);
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
		// XXX: For Clay migration plan
		// Convert Migration Range to RecordKeys
		for (Integer vertexId : migrateRanges) {
//			int startYcsbId = YcsbPartitionMetaMgr.getStartYcsbId(vertexId);
			int startYcsbId = vertexId * DATA_RANGE_SIZE;
			
			for (int i = 1; i <= MigrationManager.DATA_RANGE_SIZE; i++) {
				keyEntryMap = new HashMap<String, Constant>();
				keyEntryMap.put("ycsb_id", new VarcharConstant(
						String.format(YcsbConstants.ID_FORMAT, startYcsbId + i)));
				addOrSleep(dataSet, new RecordKey("ycsb", keyEntryMap));
			}
		}
		System.out.println("Migrate from Total " + dataSet.size() + "Keys");

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
	public int recordSize(String tableName) {
		switch (tableName) {
		case "ycsb":
			return 1000;
		default:
			throw new IllegalArgumentException("No such table for YCSB");
		}
	}

}