package org.elasql.bench.server.metadata;

import org.elasql.bench.micro.ElasqlMicrobenchConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;

public class MicroBenchPartitionMetaMgr extends PartitionMetaMgr {

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

		if (iidCon != null) {

			if (Elasql.migrationMgr().keyIsInMigrationRange(key)) {
				if (Elasql.migrationMgr().isMigrated())
					return Elasql.migrationMgr().getDestPartition();
				else {
					//before migrated
					if (!Elasql.migrationMgr().isMigrating())
						return Elasql.migrationMgr().getSourcePartition();
					else {
						if (Elasql.migrationMgr().isRecordMigrated(key))
							return Elasql.migrationMgr().getDestPartition();
						else
							return Elasql.migrationMgr().getSourcePartition();
					}

				}
			}

		} else {
			// Fully replicated
			return Elasql.serverId();
		}

		return getRangeIndex(key);
		// return key.hashCode() % NUM_PARTITIONS;
	}
}
