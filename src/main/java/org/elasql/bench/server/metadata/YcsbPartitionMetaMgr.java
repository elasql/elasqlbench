package org.elasql.bench.server.metadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class YcsbPartitionMetaMgr extends PartitionMetaMgr {
	
	public YcsbPartitionMetaMgr() {
		if (PartitionMetaMgr.USE_SCHISM) {
			//shoud load metis when loading testbed
			getLocationFromMetis();
			//monitor should commit it
		}
	}

	public void getLocationFromMetis() {
		try (BufferedReader br = new BufferedReader(new FileReader("/opt/shared/metis_ycsb_table.part"))) {

			String sCurrentLine;
			Map<String, Constant> keyEntryMap;
			int line_c = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				for (int i = 1; i <= MigrationManager.dataRange; i++) {
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("ycsb_id", new VarcharConstant(
							String.format(YcsbConstants.ID_FORMAT, MigrationManager.dataRange * line_c + i)));
					this.setPartition(new RecordKey("ycsb", keyEntryMap), Integer.parseInt(sCurrentLine));
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
