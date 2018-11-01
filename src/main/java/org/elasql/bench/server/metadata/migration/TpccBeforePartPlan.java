package org.elasql.bench.server.metadata.migration;

import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class TpccBeforePartPlan extends TpccPartitionPlan {
	
	public static final int NORMAL_WAREHOUSE_PER_PART = 10;
	
	// "HOT_WAREHOUSE_PER_HOT_PART * NUM_HOT_PARTS" should be <= NUM_PARTITIONS
	public static final int HOT_WAREHOUSE_PER_HOT_PART = 3;
	public static final int NUM_HOT_PARTS = 3;
	
	public static final int MAX_NORMAL_WID = NORMAL_WAREHOUSE_PER_PART
			* PartitionMetaMgr.NUM_PARTITIONS;
	
	public int numOfWarehouses() {
		return MAX_NORMAL_WID +
				HOT_WAREHOUSE_PER_HOT_PART * NUM_HOT_PARTS;
	}
	
	/**
	 * E.g. 4 Nodes:
	 * - Node 0 {1~10,41,43}
	 * - Node 1 {11~20,42,44}
	 * - Node 2 {21~30}
	 * - Node 3 {31~40}
	 */
	public int getPartition(int wid) {
		if (wid <= MAX_NORMAL_WID)
			return (wid - 1) / NORMAL_WAREHOUSE_PER_PART;
		else
			return (wid - MAX_NORMAL_WID - 1) % HOT_WAREHOUSE_PER_HOT_PART;
	}
}