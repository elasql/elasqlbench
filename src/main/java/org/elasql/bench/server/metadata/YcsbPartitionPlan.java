package org.elasql.bench.server.metadata;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class YcsbPartitionPlan implements PartitionPlan {
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
		// Partitions each item id through mod.
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			int ycsbId = Integer.parseInt((String) idCon.asJavaVal());
			
			// Range-based
			return ycsbId / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
}
