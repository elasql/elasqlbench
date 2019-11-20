package org.elasql.bench.server.metadata;

import org.elasql.bench.micro.ElasqlMicrobenchConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class MicroBenchPartitionPlan implements PartitionPlan {
	
	private static final long serialVersionUID = 1L;

	public boolean isFullyReplicated(RecordKey key) {
		if (key.getKeyVal("i_id") != null) {
			return false;
		} else {
			return true;
		}
	}
	
	public int getPartition(RecordKey key) {
		// Partitions each item id through mod.
		Constant iidCon = key.getKeyVal("i_id");
		if (iidCon != null) {
			int iid = (int) iidCon.asJavaVal();
			return (iid - 1) / ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}

	@Override
	public PartitionPlan getBasePartitionPlan() {
		return this;
	}

	@Override
	public boolean isBasePartitionPlan() {
		return true;
	}

	@Override
	public void changeBasePartitionPlan(PartitionPlan plan) {
		throw new RuntimeException("There is no base partition plan in "
				+ "MicroBenchPartitionPlan that can be changed");
	}

	@Override
	public int numberOfPartitions() {
		return PartitionMetaMgr.NUM_PARTITIONS;
	}
}
