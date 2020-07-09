package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.sql.RecordKeyBuilder;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

/**
 * Partitions each table on warehouse id.
 */
public class TpccPartitionPlan extends PartitionPlan {

	public boolean isFullyReplicated(RecordKey key) {
		return key.getTableName().equals("item");
	}
	
	public static Integer getWarehouseId(RecordKey key) {
		// For other tables, partitioned by wid
		Constant widCon;
		switch (key.getTableName()) {
		case "warehouse":
			widCon = key.getVal("w_id");
			break;
		case "district":
			widCon = key.getVal("d_w_id");
			break;
		case "stock":
			widCon = key.getVal("s_w_id");
			break;
		case "customer":
			widCon = key.getVal("c_w_id");
			break;
		case "history":
			widCon = key.getVal("h_c_w_id");
			break;
		case "orders":
			widCon = key.getVal("o_w_id");
			break;
		case "new_order":
			widCon = key.getVal("no_w_id");
			break;
		case "order_line":
			widCon = key.getVal("ol_w_id");
			break;
		default:
			return null;
		}
		
		return (Integer) widCon.asJavaVal();
	}
	
	public int numOfWarehouses() {
		return ElasqlTpccConstants.WAREHOUSE_PER_PART * PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public int getPartition(int wid) {
		return (wid - 1) / ElasqlTpccConstants.WAREHOUSE_PER_PART;
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

	@Override
	public PartitionPlan getBasePlan() {
		return this;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("TPC-C range partition (each range has %d warehouses)", ElasqlTpccConstants.WAREHOUSE_PER_PART);
	}

	@Override
	public RecordKey getPartitioningKey(RecordKey key) {
		RecordKeyBuilder builder;
		
		switch (key.getTableName()) {
		case "warehouse":
			builder = new RecordKeyBuilder("warehouse");
			builder.addFldVal("w_id", key.getVal("w_id"));
			break;
		case "district":
			builder = new RecordKeyBuilder("district");
			builder.addFldVal("d_w_id", key.getVal("d_w_id"));
			break;
		case "stock":
			builder = new RecordKeyBuilder("stock");
			builder.addFldVal("s_w_id", key.getVal("s_w_id"));
			break;
		case "customer":
			builder = new RecordKeyBuilder("customer");
			builder.addFldVal("c_w_id", key.getVal("c_w_id"));
			break;
		case "history":
			builder = new RecordKeyBuilder("history");
			builder.addFldVal("h_c_w_id", key.getVal("h_c_w_id"));
			break;
		case "orders":
			builder = new RecordKeyBuilder("orders");
			builder.addFldVal("o_w_id", key.getVal("o_w_id"));
			break;
		case "new_order":
			builder = new RecordKeyBuilder("new_order");
			builder.addFldVal("no_w_id", key.getVal("no_w_id"));
			break;
		case "order_line":
			builder = new RecordKeyBuilder("order_line");
			builder.addFldVal("ol_w_id", key.getVal("ol_w_id"));
			break;
		default:
			throw new RuntimeException("Unknown table " + key.getTableName());
		}

		return builder.build();
	}
}
