package org.elasql.bench.server.metadata.ycsb;

import org.elasql.bench.server.migraion.YcsbSingleTenantScaleInMigraPlan;
import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.migration.MigrationPlan;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.elasql.storage.metadata.ScalablePartitionPlan;
import org.vanilladb.core.sql.Constant;

// Only works for org.elasql.bench.rte.ycsb.SingleHotTenantParamGen
public class YcsbSingleTenantScaleOutPartPlan extends PartitionPlan implements ScalablePartitionPlan {
	
	private static final long serialVersionUID = 1L;

	private final int numOfParts;
	
	private final int hotTenantStartId; // Inclusive
	private final int hotTenantEndId; // Inclusive
	
	public YcsbSingleTenantScaleOutPartPlan(int numberOfPartitions, int tenantsPerPart) {
		this.numOfParts = numberOfPartitions;
		
		int recordPerTenant = ElasqlYcsbConstants.RECORD_PER_PART / tenantsPerPart;
		this.hotTenantStartId = 1;
		this.hotTenantEndId = recordPerTenant;
	}
	
	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	@Override
	public int getPartition(RecordKey key) {
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			int ycsbId = Integer.parseInt((String) idCon.asJavaVal());

			// Check if the key is in the hot tenant
			if (ycsbId >= hotTenantStartId && ycsbId <= hotTenantEndId)
				return numOfParts - 1;
			else
				// Range-based
				return ycsbId / ElasqlYcsbConstants.RECORD_PER_PART;
		} else {
			throw new RuntimeException("Cannot find partition for " + key);
		}
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
				+ "YcsbScaleOutPlan that can be changed");
	}
	
	@Override
	public String toString() {
		return String.format("YCSB Single Hot Tenant Scale-out Partition: "
				+ "[%d ~ %d on partition %d, the others follow the original range partition]",
				hotTenantStartId, hotTenantEndId, numOfParts - 1);
	}
	
	public int getHotTenantStartId() {
		return hotTenantStartId;
	}
	
	public int getHotTenantEndId() {
		return hotTenantEndId;
	}

	@Override
	public MigrationPlan scaleOut() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public MigrationPlan scaleIn() {
		return new YcsbSingleTenantScaleInMigraPlan(this);
	}
}
