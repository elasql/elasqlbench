package org.elasql.bench.server.metadata;

import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class TpcePartitionPlan extends PartitionPlan {

	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	@Override
	public int getPartition(RecordKey key) {
		String fld = key.getFields()[0];
		Constant val = key.getKeyVal(fld);
		return Math.abs(val.hashCode() % PartitionMetaMgr.NUM_PARTITIONS);
	}

}
