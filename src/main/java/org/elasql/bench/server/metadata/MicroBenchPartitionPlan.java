package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.micro.ElasqlMicrobenchConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.sql.RecordKeyBuilder;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class MicroBenchPartitionPlan extends PartitionPlan {
	
	public Integer getItemId(RecordKey key) {
		Constant iidCon = key.getVal("i_id");
		if (iidCon != null) {
			return (Integer) iidCon.asJavaVal();
		} else {
			return null;
		}
	}
	
	public boolean isFullyReplicated(RecordKey key) {
		if (key.getVal("i_id") != null) {
			return false;
		} else {
			return true;
		}
	}
	
	public int getPartition(int iid) {
		return (iid - 1) / ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
	}
	
	public int getPartition(RecordKey key) {
		Integer iid = getItemId(key);
		if (iid != null) {
			return getPartition(iid);
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}

	@Override
	public PartitionPlan getBasePlan() {
		return this;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		new UnsupportedOperationException();
	}

	@Override
	public RecordKey getPartitioningKey(RecordKey key) {
		if (key.getTableName().equals("item"))
			return key;
		throw new RuntimeException("Unknown table " + key.getTableName());
	}
}
