package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionPlan;

public class YcsbMultiTenantsPartitionPlan extends PartitionPlan {
	
	public static int getTenantId(PrimaryKey key) {
		String tableName = key.getTableName();
		if (!tableName.startsWith("ycsb"))
			throw new IllegalArgumentException("does not recongnize " + key);
		return Integer.parseInt(tableName.substring(4));
	}
	
	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		return false;
	}

	@Override
	public int getPartition(PrimaryKey key) {
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
