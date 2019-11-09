package org.elasql.bench.migration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.server.migration.MigrationPlan;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.tpcc.TpccTransactionType;

public class TpccMigrationManager extends MigrationManager {
	private static Logger logger = Logger.getLogger(TpccMigrationManager.class.getName());

	public static final long RECORD_PERIOD = 5000;
	private static final int COUNTS_FOR_SLEEP = 10000;
	
	private int dataSetCounter = 0;

	public TpccMigrationManager(int nodeId) {
		super(RECORD_PERIOD, nodeId);
	}

	@Override
	public RecordKey getRepresentative(RecordKey key) {
		// No grouping
		return key;
	}

	@Override
	public long getWaitingTime() {
		return BenchmarkerParameters.WARM_UP_INTERVAL;
//		return 10;
	}

	@Override
	public long getMigrationPreiod() {
		return 60 * 1000;
	}

	@Override
	public long getMigrationStopTime() {
		return BenchmarkerParameters.WARM_UP_INTERVAL + BenchmarkerParameters.BENCHMARK_INTERVAL;
	}
	
	@Override
	protected List<MigrationPlan> generateScalingOutColdMigrationPlans() {
		throw new RuntimeException("Method unimplemented");
	}
	
	@Override
	protected List<MigrationPlan> generateConsolidationColdMigrationPlans(int targetPartition) {
		throw new RuntimeException("Method unimplemented");
	}

	@Override
	public void sendLaunchClayReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, TpccTransactionType.LAUNCH_CLAY.ordinal(), (Object[]) null) };
		// Cannot be called from Appia thread
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}

	@Override
	public void broadcastMigrateKeys(Object[] params) {
		Object[] call;
		call = new Object[] {
				new StoredProcedureCall(-1, -1, TpccTransactionType.BROADCAST_MIGRAKEYS.ordinal(), params) };
		// Cannot be called from Appia thread
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}

	@Override
	public void onReceiveStartMigrationReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = {
				new StoredProcedureCall(-1, -1, TpccTransactionType.START_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, true);
	}

	@Override
	public void onReceiveAnalysisReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = {
				new StoredProcedureCall(-1, -1, TpccTransactionType.MIGRATION_ANALYSIS.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}

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
					new StoredProcedureCall(-1, -1, TpccTransactionType.ASYNC_MIGRATE.ordinal(), params) };
		} else
			call = new Object[] {
					new StoredProcedureCall(-1, -1, TpccTransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, true);
	}

	@Override
	public void onReceiveStopMigrateReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = {
				new StoredProcedureCall(-1, -1, TpccTransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		Elasql.connectionMgr().sendBroadcastRequest(call, true);
	}

	@Override
	public void prepareAnalysis() {
		// Do nothing
	}

	@Override
	public int recordSize(String tableName) {
		switch(tableName){
		case "warehouse":
			return 344;
		case "district":
			return 352;
		case "customer":
			return 2552;
		case "history":
			return 132;
		case "new_order":
			return 12;
		case "orders":
			return 36;
		case "order_line":
			return 140;
		case "item":
			return 320;
		case "stock":
			return 1184;
		default:
			throw new IllegalArgumentException("No such table for TPCC");
		}
	}

	@Override
	public Map<RecordKey, Boolean> generateDataSetForMigration() {
		// The map records the migration data set
		Map<RecordKey, Boolean> dataSet = new HashMap<RecordKey, Boolean>();

		// Convert group keys to individual RecordKey
		for (RecordKey represetKey : migratingGroups) {
			addOrSleep(dataSet, represetKey);
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Analysis completes. Will migrate " + dataSet.size() + " keys.");

		return dataSet;
	}

	private void addOrSleep(Map<RecordKey, Boolean> map, RecordKey key) {
		dataSetCounter++;

		if (dataSetCounter % COUNTS_FOR_SLEEP == 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		map.put(key, Boolean.FALSE);
	}

}
