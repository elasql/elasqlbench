package org.elasql.bench.server.migration.tpcc;

import java.util.ArrayList;
import java.util.List;

import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.storage.metadata.PartitionPlan;

public class TpccMigrationPlan implements MigrationPlan {
	
	private static final long serialVersionUID = 20210602001L;
	
	private TpccPartitionPlan beforePlan;
	private TpccPartitionPlan afterPlan;
	
	public TpccMigrationPlan(TpccPartitionPlan beforePlan, TpccPartitionPlan afterPlan) {
		this.beforePlan = beforePlan;
		this.afterPlan = afterPlan;
	}
	
	@Override
	public PartitionPlan getNewPart() {
		return afterPlan;
	}
	
	@Override
	public List<MigrationRange> getMigrationRanges(MigrationComponentFactory factory) {
		List<MigrationRange> migrationRanges = new ArrayList<MigrationRange>();
		for (int wid = 1; wid <= beforePlan.numOfWarehouses(); wid++) {
			int beforePartId = beforePlan.getPartition(wid);
			int afterPartId = afterPlan.getPartition(wid);
			
			if (beforePartId != afterPartId) {
				migrationRanges.add(new TpccMigrationRange(wid, wid, beforePartId, afterPartId));
			}
		}
		return migrationRanges;
	}
	
	@Override
	public List<MigrationPlan> splits() {
		throw new UnsupportedOperationException("Unimplemented");
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		
		sb.append("TPC-C Migration Plan: [");
		for (int wid = 1; wid <= beforePlan.numOfWarehouses(); wid++) {
			int beforePartId = beforePlan.getPartition(wid);
			int afterPartId = afterPlan.getPartition(wid);
			
			if (beforePartId != afterPartId) {
				if (!first)
					sb.append(", ");
				sb.append(String.format("Warehouse #%d from part.%d to part%d",
						wid, beforePartId, afterPartId));
				first = false;
			}
		}
		sb.append("]");
		
		return sb.toString();
	}
}
