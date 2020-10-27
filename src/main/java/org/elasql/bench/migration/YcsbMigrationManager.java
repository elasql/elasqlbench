package org.elasql.bench.migration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.rte.ycsb.MultiTanentsParamGen;
import org.elasql.bench.rte.ycsb.google.GoogleComplexWorkloadsParamGen;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.bench.ycsb.ElasqlYcsbConstants.WorkloadType;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.server.migration.MigrationPlan;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.micro.MicroTransactionType;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class YcsbMigrationManager extends MigrationManager {
	private static Logger logger = Logger.getLogger(YcsbMigrationManager.class.getName());

	public static final long RECORD_PERIOD = 3000;
	private static final int COUNTS_FOR_SLEEP = 10000;
	
	private static final int GROUP_SIZE = PartitionMetaMgr.USE_SCHISM? 1: // [Schism: Clay]
		(ENABLE_NODE_SCALING? (IS_SCALING_OUT? 100: 1000) : 500);
	
	private int parameterCounter = 0;
	
//	public static final int VERTEX_PER_PART = ElasqlYcsbConstants.RECORD_PER_PART / DATA_RANGE_SIZE;
	
	private static RecordKey toRecordKey(int ycsbId) {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ycsb_id", new VarcharConstant(
				String.format(YcsbConstants.ID_FORMAT, ycsbId)));
		return new RecordKey("ycsb", keyEntryMap);
	}
	
	private static int toYcsbId(RecordKey key) {
		return Integer.parseInt(key.getKeyVal("ycsb_id").toString());
	}

	public YcsbMigrationManager(int nodeId) {
		super(RECORD_PERIOD, nodeId);
	}
	
	public RecordKey getRepresentative(RecordKey key) {
		int ycsbId = toYcsbId(key);
		// E.g. 1~10 => 1, 11~20 => 11 for GROUP_SIZE = 10
		int representativeId = ((ycsbId - 1) / GROUP_SIZE) * GROUP_SIZE + 1;
		return toRecordKey(representativeId);
	}
	
	@Override
	public long getWaitingTime() {
		// 30 seconds for the delay of starting sending transactions
		if (PartitionMetaMgr.USE_SCHISM)
			return GoogleComplexWorkloadsParamGen.WARMUP_TIME + 30 * 1000 + 120 * 1000;
		else {
			if (ElasqlYcsbConstants.WORKLOAD_TYPE == WorkloadType.GOOGLE) {
				return GoogleComplexWorkloadsParamGen.WARMUP_TIME + 30 * 1000;
			} else if (ElasqlYcsbConstants.WORKLOAD_TYPE == WorkloadType.MULTI_TENANTS) {
				return MultiTanentsParamGen.WARMUP_TIME + 30 * 1000;
			} else {
				throw new RuntimeException("Not implemented");	
			}
		}
//			return 1000 * 1000 * 1000; // very long time
//		return 120 * 1000; // for scaling-out & consolidation
	}
	
	@Override
	public long getMigrationPreiod() {
		// For Google workloads
		if (PartitionMetaMgr.USE_SCHISM)
//			return GoogleWorkloadsParamGen.REPLAY_PREIOD - MONITORING_TIME;
			return 1000 * 1000 * 1000;
		else
			return 30 * 1000;
//		return SingleSkewWorkloadsParamGen.CHANGING_PERIOD;
//		return MultiTanentsParamGen.CHANGING_PERIOD;
//		return 1000 * 1000; // for scaling-out & consolidation
	}
	
	public long getMigrationStopTime() {
		if (ElasqlYcsbConstants.WORKLOAD_TYPE == WorkloadType.GOOGLE) {
			return GoogleComplexWorkloadsParamGen.REPLAY_TIME + 30 * 1000;
		} else {
			return BenchmarkerParameters.WARM_UP_INTERVAL + BenchmarkerParameters.BENCHMARK_INTERVAL;
		}
//		return 400 * 1000;
//		return 1000 * 1000; // only stop after long time (scaling-out & consolidation, multitanent)
	}
	
	// XXX: This plan only works on multi-tenant scenario with 4 server nodes
	@Override
	protected List<MigrationPlan> generateScalingOutColdMigrationPlans() {
		List<MigrationPlan> plans = new LinkedList<MigrationPlan>();
		int recordPerPart = ElasqlYcsbConstants.RECORD_PER_PART;
		int recordPerTenant = ElasqlYcsbConstants.RECORD_PER_PART;
		
		for (int partId = 0; partId < 3; partId++) {
			int startId = partId * recordPerPart + 1;
			int endId = partId * recordPerPart + recordPerTenant;
			for (int representId = startId; representId <= endId; representId += GROUP_SIZE) {
				MigrationPlan p = new MigrationPlan(partId, 3);
				for (int k = startId; k <= endId; k++)
					p.addKey(toRecordKey(representId));
				plans.add(p);
			}
		}
		
		return plans;
	}

	// XXX: This plan only works on multi-tenant scenario with 4 server nodes
	@Override
	protected List<MigrationPlan> generateConsolidationColdMigrationPlans(int targetPartition) {
		if (targetPartition == -1)
			return generateSeprateConsolidationPlans();
		return generateOneToOneConsolidationPlans(targetPartition);
	}
	
	private List<MigrationPlan> generateSeprateConsolidationPlans() {
		List<MigrationPlan> plans = new LinkedList<MigrationPlan>();
		int recordPerPart = ElasqlYcsbConstants.RECORD_PER_PART;
		MigrationPlan p0 = new MigrationPlan(3, 0);
		MigrationPlan p1 = new MigrationPlan(3, 1);
		MigrationPlan p2 = new MigrationPlan(3, 2);
		
		int lastPartStartId = 3 * recordPerPart + 1;
		int eachPartGotCount = recordPerPart / 3;
		
		for (int offsetId = 0; offsetId < recordPerPart; offsetId += GROUP_SIZE) {
			if (offsetId < eachPartGotCount) {
				p0.addKey(toRecordKey(lastPartStartId + offsetId));
			} else if (offsetId < eachPartGotCount * 2) { 
				p1.addKey(toRecordKey(lastPartStartId + offsetId));
			} else {
				p2.addKey(toRecordKey(lastPartStartId + offsetId));
			}
		}
		
		plans.add(p0);
		plans.add(p1);
		plans.add(p2);
		
		return plans;
	}
	
	private List<MigrationPlan> generateOneToOneConsolidationPlans(int targetPartition) {
		List<MigrationPlan> plans = new LinkedList<MigrationPlan>();
		int recordPerPart = ElasqlYcsbConstants.RECORD_PER_PART;
		MigrationPlan p = new MigrationPlan(3, targetPartition);
		
		int lastPartStartId = 3 * recordPerPart + 1;
		for (int offsetId = 0; offsetId < recordPerPart; offsetId += GROUP_SIZE)
			p.addKey(toRecordKey(lastPartStartId + offsetId));
		plans.add(p);
		
		return plans;
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

		// Convert group keys to individual RecordKey
		for (RecordKey represetKey : migratingGroups) {
			
			if (logger.isLoggable(Level.FINE))
				logger.fine("Add represented key for migration: " + represetKey);
			
			int startYcsbId = toYcsbId(represetKey);
			for (int offset = 0; offset < GROUP_SIZE; offset++) {
				addOrSleep(dataSet, toRecordKey(startYcsbId + offset));
			}
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Analysis completes. Will migrate " + dataSet.size() + " keys.");

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