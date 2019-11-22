package org.elasql.bench.server.metadata.ycsb;

import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class YcsbHashPartitionPlan implements PartitionPlan {

	private static final long serialVersionUID = 1L;

	private int numOfParts;

	public YcsbHashPartitionPlan(int numberOfPartitions) {
		numOfParts = numberOfPartitions;
	}

	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	@Override
	public int getPartition(RecordKey key) {
		Constant idCon = key.getKeyVal("ycsb_id");
		int id = Integer.parseInt((String) idCon.asJavaVal());
		return id % numOfParts;
	}

	@Override
	public int numberOfPartitions() {
		return numOfParts;
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
		throw new RuntimeException("There is no base partition plan in " +
				"YcsbHashPartitionPlan that can be changed");
	}

}
