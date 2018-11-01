package org.elasql.bench.server.migration.tpcc;

import java.util.List;

import org.elasql.bench.server.metadata.migration.TpccAfterPartPlan;
import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationRange;
import org.elasql.storage.metadata.PartitionPlan;

public class TpccMigrationMgr extends MigrationMgr {

	@Override
	public List<MigrationRange> generateMigrationRanges(PartitionPlan newPlan) {
		TpccAfterPartPlan tpccNewPlan = (TpccAfterPartPlan) newPlan;
		return tpccNewPlan.generateMigrationRanges();
	}

}
