package org.elasql.bench.server.metadata;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;

public class YcsbPartitionMetaMgr extends PartitionMetaMgr {
	
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
