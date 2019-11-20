package org.elasql.bench.server.migraion;

import java.util.ArrayDeque;
import java.util.Deque;

import org.elasql.bench.server.metadata.ycsb.YcsbRangePartitionPlan;
import org.elasql.bench.server.metadata.ycsb.YcsbSingleTenantScaleOutPartPlan;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;

public class YcsbSingleTenantScaleInMigraPlan implements MigrationPlan {
	
	private static final long serialVersionUID = 1L;

	private YcsbSingleTenantScaleOutPartPlan oldPartitionPlan;
	private YcsbRangePartitionPlan newPartitionPlan;
	
	public YcsbSingleTenantScaleInMigraPlan(YcsbSingleTenantScaleOutPartPlan oldPartPlan) {
		oldPartitionPlan = oldPartPlan;
		newPartitionPlan = new YcsbRangePartitionPlan(PartitionMetaMgr.NUM_PARTITIONS - 1);
	}

	@Override
	public PartitionPlan oldPartitionPlan() {
		return oldPartitionPlan;
	}

	@Override
	public PartitionPlan newPartitionPlan() {
		return newPartitionPlan;
	}

	@Override
	public Deque<MigrationRange> generateMigrationRanges() {
		Deque<MigrationRange> ranges = new ArrayDeque<MigrationRange>();
		
		ranges.add(new MigrationRange("ycsb", "ycsb_id", 
				oldPartitionPlan.getHotTenantStartId(),
				oldPartitionPlan.getHotTenantEndId(),
				PartitionMetaMgr.NUM_PARTITIONS - 1, 0));
		
		return ranges;
	}
}
