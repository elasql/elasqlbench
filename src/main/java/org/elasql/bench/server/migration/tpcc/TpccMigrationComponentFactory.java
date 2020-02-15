package org.elasql.bench.server.migration.tpcc;

import java.util.ArrayList;
import java.util.List;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccBenchmark;
import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.bench.server.metadata.migration.TpccAfterPartPlan;
import org.elasql.bench.server.metadata.migration.scaleout.TpccScaleoutAfterPartPlan;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationRange;
import org.elasql.storage.metadata.PartitionPlan;

public class TpccMigrationComponentFactory extends MigrationComponentFactory {
	
	public List<MigrationRange> generateMigrationRanges(PartitionPlan oldPlan, PartitionPlan newPlan) {
		TpccPartitionPlan tpccOldPlan = (TpccPartitionPlan) oldPlan;
		TpccPartitionPlan tpccNewPlan = (TpccPartitionPlan) newPlan;
		List<MigrationRange> list = new ArrayList<MigrationRange>();
		
		for (int wid = 1; wid <= tpccOldPlan.numOfWarehouses(); wid++) {
			int sourceNodeId = tpccOldPlan.getPartition(wid);
			int destNodeId = tpccNewPlan.getPartition(wid);
			if (sourceNodeId != destNodeId)
				list.add(new TpccMigrationRange(wid, wid, sourceNodeId, destNodeId));
		}
		
		return list;
	}
	
	public PartitionPlan newPartitionPlan() {
		if (ElasqlTpccBenchmark.ENABLE_SCALE_OUT_TEST)
			return new TpccScaleoutAfterPartPlan();
		else
			return new TpccAfterPartPlan();	
	}

}
