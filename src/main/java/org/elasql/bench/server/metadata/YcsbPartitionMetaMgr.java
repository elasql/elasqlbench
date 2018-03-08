package org.elasql.bench.server.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.migration.YcsbMigrationManager;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;


public class YcsbPartitionMetaMgr extends PartitionMetaMgr {
	private static Logger logger = Logger.getLogger(YcsbPartitionMetaMgr.class.getName());
	
	private static final String LOC_FILE_PATH = "/opt/shared/metis_ycsb_table.part";
	
	public YcsbPartitionMetaMgr() {
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

			String line;
			Map<String, Constant> keyEntryMap;
			int lineCount = 0;
			while ((line = br.readLine()) != null) {
				int newPartId = Integer.parseInt(line);
				int higherPart = lineCount / YcsbMigrationManager.VERTEX_PER_PART; // 123 => 1
				int lowerPart = lineCount % YcsbMigrationManager.VERTEX_PER_PART; // 123 => 23
				int startYcsbId = higherPart * ElasqlYcsbConstants.MAX_RECORD_PER_PART + lowerPart * YcsbMigrationManager.DATA_RANGE_SIZE; // 1 * 1000000000 + 23 * 10000
				
				for (int i = 1; i <= MigrationManager.DATA_RANGE_SIZE; i++) {
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("ycsb_id", new VarcharConstant(
							String.format(YcsbConstants.ID_FORMAT, startYcsbId + i)));
					this.setPartition(new RecordKey("ycsb", keyEntryMap), newPartId);
				}
				lineCount++;

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}
	
	public static int getRangeIndex(RecordKey key) {
		return getRangeIndex(Integer.parseInt(key.getKeyVal("ycsb_id").toString()));
	}

	public static int getRangeIndex(int id) {
		return (id - 1) / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
	}

	public int getLocation(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */

		// For a special type of record

		if (key.getTableName().equals("notification"))
			return -1;

		Constant iidCon = key.getKeyVal("ycsb_id");

		if (iidCon == null) {
			// Fully replicated
			return Elasql.serverId();
		}

		return getRangeIndex(key);
		// return key.hashCode() % NUM_PARTITIONS;
	}
}
