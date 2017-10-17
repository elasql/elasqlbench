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

	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Ycsb-benchmark testbed.
		 * Partitions each item id through mod.
		 */
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			String id = (String) idCon.asJavaVal();
			return (Integer.parseInt(id) - 1) / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
}
