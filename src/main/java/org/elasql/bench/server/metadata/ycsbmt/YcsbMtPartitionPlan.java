package org.elasql.bench.server.metadata.ycsbmt;

import org.elasql.migration.MigrationPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.elasql.storage.metadata.ScalablePartitionPlan;

public class YcsbMtPartitionPlan implements ScalablePartitionPlan {
	
	private static final long serialVersionUID = 20201110001L;
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
		if (key.getTableName().startsWith("ycsb")) {
			// Use the table id to partition
			int tableId = getTableId(key.getTableName());
			return tableId;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
	
	@Override
	public int numberOfPartitions() {
		return PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	@Override
	public String toString() {
		return String.format("YCSB-MultiTable Partition: each partition has its own YCSB table.");
	}

	@Override
	public MigrationPlan scaleOut() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public MigrationPlan scaleIn() {
		throw new RuntimeException("Not implemented");
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
				+ "YcsbMtPartitionPlan that can be changed");
	}
	
	private int getTableId(String tableName) {
		return Integer.parseInt(tableName.substring(5));
	}
}
