package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;

public class YcsbMultiTenantsPartitionPlan extends PartitionPlan {
	
	public static int getTenantId(RecordKey key) {
		String tableName = key.getTableName();
		if (!tableName.startsWith("ycsb"))
			throw new IllegalArgumentException("does not recongnize " + key);
		return Integer.parseInt(tableName.substring(4));
	}
	
	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	@Override
	public int getPartition(RecordKey key) {
		int tenantId = getTenantId(key);
		return tenantId / ElasqlYcsbConstants.TENANTS_PER_PART;
	}

	@Override
	public PartitionPlan getBasePlan() {
		return this;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		new UnsupportedOperationException();
	}

}
