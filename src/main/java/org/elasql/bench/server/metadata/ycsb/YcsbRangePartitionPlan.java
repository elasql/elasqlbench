package org.elasql.bench.server.metadata.ycsb;

import org.elasql.bench.server.migraion.YcsbSingleTenantScaleOutMigraPlan;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.migration.MigrationPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.elasql.storage.metadata.ScalablePartitionPlan;
import org.vanilladb.core.sql.Constant;

public class YcsbRangePartitionPlan implements ScalablePartitionPlan {
	
	private static final long serialVersionUID = 1L;
	
	private final int numOfParts;
	
	public YcsbRangePartitionPlan(int numberOfPartitions) {
		numOfParts = numberOfPartitions;
	}
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
		// Partitions each item id through mod.
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			int ycsbId = Integer.parseInt((String) idCon.asJavaVal());
			
			// Range-based
			return (ycsbId - 1) / ElasqlYcsbConstants.RECORD_PER_PART;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
	
	@Override
	public int numberOfPartitions() {
		return numOfParts;
	}
	
	@Override
	public String toString() {
		return String.format("YCSB Range Partition: [%d partitions on 'ycsb_id', each partition has %d records]",
				numOfParts, ElasqlYcsbConstants.RECORD_PER_PART);
	}

	@Override
	public MigrationPlan scaleOut() {
		return new YcsbSingleTenantScaleOutMigraPlan(this);
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
				+ "YcsbRangePartitionPlan that can be changed");
	}
}
