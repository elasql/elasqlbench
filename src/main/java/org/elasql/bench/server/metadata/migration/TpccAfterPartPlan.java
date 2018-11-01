package org.elasql.bench.server.metadata.migration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.elasql.migration.MigrationRange;

public class TpccAfterPartPlan extends TpccBeforePartPlan implements Serializable {
	
	private static final long serialVersionUID = 20181031001l;

	/**
	 * E.g. 4 Nodes:
	 * - Node 0 {1~10,41}
	 * - Node 1 {11~20,42}
	 * - Node 2 {21~30,43}
	 * - Node 3 {31~40,44}
	 */
	public int getPartition(int wid) {
		if (wid < MAX_NORMAL_WID)
			return (wid - 1) / NORMAL_WAREHOUSE_PER_PART;
		else
			return (wid - MAX_NORMAL_WID - 1) / HOT_WAREHOUSE_PER_HOT_PART;
	}
	
	public List<MigrationRange> generateMigrationRanges() {
		List<MigrationRange> list = new ArrayList<MigrationRange>();
		
		for (int wid = MAX_NORMAL_WID + 1; wid <= numOfWarehouses(); wid++) {
			list.add(new TpccMigrationRange(wid, wid, super.getPartition(wid), getPartition(wid)));
		}
		
		return list;
	}
}