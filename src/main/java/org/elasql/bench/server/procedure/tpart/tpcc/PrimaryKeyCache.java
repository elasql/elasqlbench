package org.elasql.bench.server.procedure.tpart.tpcc;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccConstants;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.core.sql.IntegerConstant;

public class PrimaryKeyCache {
	
	private static final int WAREHOUSE_COUNT = ElasqlTpccConstants.WAREHOUSE_PER_PART
			* PartitionMetaMgr.NUM_PARTITIONS;
	private static final int DISTRICTS_PER_WAREHOUSE = TpccConstants.DISTRICTS_PER_WAREHOUSE;
	private static final int CUSTOMERS_PER_DISTRICT = TpccConstants.CUSTOMERS_PER_DISTRICT;
	private static final int ITEM_COUNT = TpccConstants.NUM_ITEMS;

	private static PrimaryKey[] warehouseKeys;
	private static PrimaryKey[][] districtKeys;
	private static PrimaryKey[][][] customerKeys;
	private static PrimaryKey[] itemKeys;
	
	static {
		buildWarehouseKeyCache();
		buildDistrictKeyCache();
		buildCustomerKeyCache();
		buildItemKeyCache();
	}
	
	public static PrimaryKey getWarehouseKey(int wid) {
		return warehouseKeys[wid - 1];
	}
	
	public static PrimaryKey getDistrictKey(int wid, int did) {
		return districtKeys[wid - 1][did - 1];
	}
	
	public static PrimaryKey getCustomerKey(int wid, int did, int cid) {
		return customerKeys[wid - 1][did - 1][cid - 1];
	}
	
	public static PrimaryKey getItemKey(int iid) {
		return itemKeys[iid - 1];
	}
	
	private static void buildWarehouseKeyCache() {
		PrimaryKeyBuilder builder = new PrimaryKeyBuilder("warehouse");
		builder.addFldVal("w_id", new IntegerConstant(1));
		
		warehouseKeys = new PrimaryKey[WAREHOUSE_COUNT];
		for (int wid = 1; wid <= WAREHOUSE_COUNT; wid++) {
			builder.setVal("w_id", new IntegerConstant(wid));
			warehouseKeys[wid - 1] = builder.build();
		}
	}
	
	private static void buildDistrictKeyCache() {
		PrimaryKeyBuilder builder = new PrimaryKeyBuilder("district");
		builder.addFldVal("d_w_id", new IntegerConstant(1));
		builder.addFldVal("d_id", new IntegerConstant(1));
		
		districtKeys = new PrimaryKey[WAREHOUSE_COUNT][DISTRICTS_PER_WAREHOUSE];
		for (int wid = 1; wid <= WAREHOUSE_COUNT; wid++) {
			builder.setVal("d_w_id", new IntegerConstant(wid));
			for (int did = 1; did <= DISTRICTS_PER_WAREHOUSE; did++) {
				builder.setVal("d_id", new IntegerConstant(did));
				districtKeys[wid - 1][did - 1] = builder.build();
			}
		}
	}
	
	private static void buildCustomerKeyCache() {
		PrimaryKeyBuilder builder = new PrimaryKeyBuilder("customer");
		builder.addFldVal("c_w_id", new IntegerConstant(1));
		builder.addFldVal("c_d_id", new IntegerConstant(1));
		builder.addFldVal("c_id", new IntegerConstant(1));
		
		customerKeys = new PrimaryKey[WAREHOUSE_COUNT]
				[DISTRICTS_PER_WAREHOUSE][CUSTOMERS_PER_DISTRICT];
		for (int wid = 1; wid <= WAREHOUSE_COUNT; wid++) {
			builder.setVal("c_w_id", new IntegerConstant(wid));
			for (int did = 1; did <= DISTRICTS_PER_WAREHOUSE; did++) {
				builder.setVal("c_d_id", new IntegerConstant(did));
				for (int cid = 1; cid <= CUSTOMERS_PER_DISTRICT; cid++) {
					builder.setVal("c_id", new IntegerConstant(cid));
					customerKeys[wid - 1][did - 1][cid - 1] = builder.build();
				}
			}
		}
	}
	
	private static void buildItemKeyCache() {
		PrimaryKeyBuilder builder = new PrimaryKeyBuilder("item");
		builder.addFldVal("i_id", new IntegerConstant(1));
		
		itemKeys = new PrimaryKey[ITEM_COUNT];
		for (int iid = 1; iid <= ITEM_COUNT; iid++) {
			builder.setVal("i_id", new IntegerConstant(iid));
			itemKeys[iid - 1] = builder.build();
		}
	}
}
