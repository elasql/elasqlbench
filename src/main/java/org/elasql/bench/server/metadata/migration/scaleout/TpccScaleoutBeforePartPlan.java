package org.elasql.bench.server.metadata.migration.scaleout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class TpccScaleoutBeforePartPlan extends TpccPartitionPlan implements Serializable {
	
	private static final long serialVersionUID = 20210602002l;
	
	public static final int NUM_HOT_PARTS;
	public static final int HOT_WAREHOUSE_PER_HOT_PART;
	
	static {
		NUM_HOT_PARTS = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(TpccScaleoutBeforePartPlan.class.getName() + ".NUM_HOT_PARTS", 1);
		HOT_WAREHOUSE_PER_HOT_PART = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(TpccScaleoutBeforePartPlan.class.getName() + ".HOT_WAREHOUSE_PER_HOT_PART", 1);
		
		int minPartitionNeeded = NUM_HOT_PARTS + NUM_HOT_PARTS * HOT_WAREHOUSE_PER_HOT_PART;
		if (PartitionMetaMgr.NUM_PARTITIONS < minPartitionNeeded)
			throw new IllegalArgumentException(String.format(
					"%d partitions are not enough for a scale-out scenario with "
					+ "% hot partitions and %d hot warehouse in each partition",
					PartitionMetaMgr.NUM_PARTITIONS, NUM_HOT_PARTS, HOT_WAREHOUSE_PER_HOT_PART));
	}
	
	// we wish to distribute the hot warehouses to the empty partitions
	// each empty partition should get one hot warehouse.
	public static final int NUM_EMPTY_PARTS = NUM_HOT_PARTS * HOT_WAREHOUSE_PER_HOT_PART;
	public static final int NUM_NORMAL_PARTS = PartitionMetaMgr.NUM_PARTITIONS - NUM_HOT_PARTS - NUM_EMPTY_PARTS;

	public static final int NORMAL_WAREHOUSE_PER_PART = 10;
	
	public static final int MAX_NORMAL_WID = NORMAL_WAREHOUSE_PER_PART
			* (NUM_HOT_PARTS + NUM_NORMAL_PARTS);
	
	public int numOfWarehouses() {
		return MAX_NORMAL_WID +
				HOT_WAREHOUSE_PER_HOT_PART * NUM_HOT_PARTS;
	}
	
	/**
	 * E.g. 3 nodes with 1 hot partition which has 1 hot warehouse:
	 * - Node 0 {1~10,21}
	 * - Node 1 {11~20}
	 * - Node 2 {}
	 */
	public int getPartition(int wid) {
		if (wid <= MAX_NORMAL_WID)
			return (wid - 1) / NORMAL_WAREHOUSE_PER_PART;
		else
			return (wid - MAX_NORMAL_WID - 1) % NUM_HOT_PARTS;
	}
	
	@Override
	public String toString() {
		Map<Integer, List<Integer>> wids = new HashMap<Integer, List<Integer>>();
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++)
			wids.put(partId, new ArrayList<Integer>());
		
		for (int wid = 1; wid <= numOfWarehouses(); wid++)
			wids.get(getPartition(wid)).add(wid);

		StringBuilder sb = new StringBuilder("TPC-C Plan: { Warehouse Ids:");
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
			sb.append(String.format("Part %d:[", partId));
			
			for (Integer wid : wids.get(partId))
				sb.append(String.format("%d, ", wid));
			if (wids.get(partId).size() > 0)
				sb.delete(sb.length() - 2, sb.length());
			
			sb.append("], ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append("}");
		
		return sb.toString();
	}
}