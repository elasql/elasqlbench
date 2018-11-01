package org.elasql.bench.server.migration.tpcc;

import org.elasql.bench.server.metadata.migration.TpccAfterPartPlan;
import org.elasql.migration.MigrationSystemController;
import org.elasql.storage.metadata.PartitionPlan;

public class TpccMigrationSystemController extends MigrationSystemController {

	@Override
	public PartitionPlan newPartitionPlan() {
		return new TpccAfterPartPlan();
	}

}
