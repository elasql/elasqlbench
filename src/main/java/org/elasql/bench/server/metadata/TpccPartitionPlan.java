package org.elasql.bench.server.metadata;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.core.sql.Constant;

/**
 * Partitions each table on warehouse id.
 */
public class TpccPartitionPlan extends PartitionPlan {
	
	private int wPerPart = TpccConstants.NUM_WAREHOUSES / PartitionMetaMgr.NUM_PARTITIONS;

	public boolean isFullyReplicated(RecordKey key) {
		return key.getTableName().equals("item");
	}
	
	public static Integer getWarehouseId(RecordKey key) {
		// For other tables, partitioned by wid
		Constant widCon;
		switch (key.getTableName()) {
		case "warehouse":
			widCon = key.getKeyVal("w_id");
			break;
		case "district":
			widCon = key.getKeyVal("d_w_id");
			break;
		case "stock":
			widCon = key.getKeyVal("s_w_id");
			break;
		case "customer":
			widCon = key.getKeyVal("c_w_id");
			break;
		case "history":
			widCon = key.getKeyVal("h_c_w_id");
			break;
		case "orders":
			widCon = key.getKeyVal("o_w_id");
			break;
		case "new_order":
			widCon = key.getKeyVal("no_w_id");
			break;
		case "order_line":
			widCon = key.getKeyVal("ol_w_id");
			break;
		default:
			return null;
		}
		
		return (Integer) widCon.asJavaVal();
	}
	
	public int numOfWarehouses() {
		return TpccConstants.NUM_WAREHOUSES;
	}
	
	public int getPartition(int wid) {
		return (wid - 1) / wPerPart;
	}
	
	@Override
	public int getPartition(RecordKey key) {
		// If is item table, return self node id
		// (items are fully replicated over all partitions)
		if (key.getTableName().equals("item"))
			return Elasql.serverId();
		
		Integer wid = getWarehouseId(key);
		if (wid != null) {
			return getPartition(wid);
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
}
