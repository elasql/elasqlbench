package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class YcsbSingleTablePartitionPlan extends PartitionPlan {
	
	public static int getYcsbId(RecordKey key) {
		Constant idCon = key.getVal("ycsb_id");
		if (idCon == null)
			throw new IllegalArgumentException("does not recongnize " + key);
		return Integer.parseInt((String) idCon.asJavaVal());
	}
	
	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	@Override
	public int getPartition(RecordKey key) {
		int ycsbId = getYcsbId(key);
		return (ycsbId - 1) / ElasqlYcsbConstants.INIT_RECORD_PER_PART;
	}

	@Override
	public PartitionPlan getBasePlan() {
		return this;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		new UnsupportedOperationException();
	}
}
