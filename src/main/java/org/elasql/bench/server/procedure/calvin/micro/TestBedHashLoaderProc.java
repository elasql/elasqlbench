package org.elasql.bench.server.procedure.calvin.micro;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.micro.ElasqlMicrobenchConstants;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.tpcc.TpccConstants;
import org.vanilladb.bench.util.DoublePlainPrinter;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class TestBedHashLoaderProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(TestbedLoaderProc.class.getName());

	public TestBedHashLoaderProc(long txNum) {
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
		// XXX: This may not be a good design. Perhaps this needs its own
		// procedure.

		if (logger.isLoggable(Level.WARNING))
			logger.warning("This loading procedure only loads the data for micro-benchmarks.");

		int startIId = 1;
		int endIId = (PartitionMetaMgr.NUM_PARTITIONS) * ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
		generateItems(startIId, endIId);

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
			logger.info("Loading procedure finished.");

	}

	private void generateItems(int startIId, int endIId) {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populating items from i_id=" + startIId + " to i_id=" + endIId);

		int iid, iimid;
		String iname, idata;
		double iprice;
		String sql;

		int cout = 0;

		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		RecordKey key;
		for (int i = startIId; i <= endIId; i++) {
			keyEntryMap.clear();
			iid = i;

			keyEntryMap.put("i_id", new IntegerConstant(iid));
			key = new RecordKey("item", keyEntryMap);
			if (key.hashCode() % PartitionMetaMgr.NUM_PARTITIONS == Elasql.serverId()) {

				// Deterministic value generation by item id
				iimid = iid % (TpccConstants.MAX_IM - TpccConstants.MIN_IM) + TpccConstants.MIN_IM;
				iname = String.format("%0" + TpccConstants.MIN_I_NAME + "d", iid);
				iprice = (iid % (int) (TpccConstants.MAX_PRICE - TpccConstants.MIN_PRICE)) + TpccConstants.MIN_PRICE;
				idata = String.format("%0" + TpccConstants.MIN_I_DATA + "d", iid);
				sql = "INSERT INTO item(i_id, i_im_id, i_name, i_price, i_data) VALUES (" + iid + ", " + iimid + ", '"
						+ iname + "', " + DoublePlainPrinter.toPlainString(iprice) + ", '" + idata + "' )";

				int result = VanillaDb.newPlanner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();

				cout++;
			}
		}

		if (logger.isLoggable(Level.FINE))
			logger.info("Populating " + cout + " items completed.");
	}
}
