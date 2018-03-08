package org.elasql.bench.server.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.micro.ElasqlMicrobenchConstants;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class MicroBenchPartitionMetaMgr extends PartitionMetaMgr {
	private static Logger logger = Logger.getLogger(MicroBenchPartitionMetaMgr.class.getName());
	
	private static final String LOC_FILE_PATH = "/opt/shared/metis_micro_table.part";
	
	public MicroBenchPartitionMetaMgr() {
		if (PartitionMetaMgr.USE_SCHISM) {
			//shoud load metis when loading testbed
			loadMetisPartitions();
			//monitor should commit it
		}
	}

	public void loadMetisPartitions() {
		File file = new File(LOC_FILE_PATH);
		if (!file.exists()) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(String.format("Cannot find Metis partitions at '%s'", LOC_FILE_PATH));
			return;
		}
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			String sCurrentLine;
			Map<String, Constant> keyEntryMap;
			int line_c = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				for (int i = 1; i <= MigrationManager.DATA_RANGE_SIZE; i++) {
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("i_id", new IntegerConstant(MigrationManager.DATA_RANGE_SIZE * line_c + i));
					this.setPartition(new RecordKey("item", keyEntryMap), Integer.parseInt(sCurrentLine));
				}
				line_c++;

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public static int getRangeIndex(RecordKey key) {
		return getRangeIndex(Integer.parseInt(key.getKeyVal("i_id").toString()));
	}

	public static int getRangeIndex(int id) {
		return (id - 1) / ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
	}

	public int getLocation(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */

		// For a special type of record

		if (key.getTableName().equals("notification"))
			return -1;

		Constant iidCon = key.getKeyVal("i_id");

		if (iidCon == null) {
			// Fully replicated
			return Elasql.serverId();
		}

		return getRangeIndex(key);
		// return key.hashCode() % NUM_PARTITIONS;
	}
}
