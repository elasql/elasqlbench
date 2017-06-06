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


	public int getLocation(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */
		
		
		Constant iidCon = key.getKeyVal("i_id");
		if (iidCon != null) {
			int iid = (int) iidCon.asJavaVal();
			return (iid - 1) / ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
		
		//return key.hashCode() % NUM_PARTITIONS;
	}
}
