package org.elasql.bench.server.metadata;

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class TpccPartitionPlan implements PartitionPlan {

	public boolean isFullyReplicated(RecordKey key) {
		return key.getTableName().equals("item");
	}
	
	public static int getWarehouseId(RecordKey key) {
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
			throw new IllegalArgumentException("cannot find proper partition rule for key:" + key);
		}
		
		return (Integer) widCon.asJavaVal();
	}
	
	/**
	 * Partitions each table on warehouse id.
	 */
	public int getPartition(RecordKey key) {
		// If is item table, return self node id
		// (items are fully replicated over all partitions)
		if (key.getTableName().equals("item"))
			return Elasql.serverId();

		return getWarehouseId(key) - 1;
	}
}
