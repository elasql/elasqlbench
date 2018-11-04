package org.elasql.bench.server.migration.tpcc;

import java.util.List;

import org.elasql.bench.server.metadata.migration.TpccAfterPartPlan;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationRange;
import org.elasql.storage.metadata.PartitionPlan;

public class TpccMigrationComponentFactory extends MigrationComponentFactory {
	
	public List<MigrationRange> generateMigrationRanges(PartitionPlan newPlan) {
		TpccAfterPartPlan tpccNewPlan = (TpccAfterPartPlan) newPlan;
		return tpccNewPlan.generateMigrationRanges();
	}
	
	public PartitionPlan newPartitionPlan() {
		return new TpccAfterPartPlan();
	}

}
