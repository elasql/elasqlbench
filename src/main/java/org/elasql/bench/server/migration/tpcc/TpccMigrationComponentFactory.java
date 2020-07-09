package org.elasql.bench.server.migration.tpcc;

import org.elasql.bench.server.migration.DummyKeyIterator;
import org.elasql.bench.server.migration.SingleTableMigrationRange;
import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.planner.clay.ScatterMigrationPlan;
import org.elasql.sql.RecordKey;
import org.elasql.sql.RecordKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class TpccMigrationComponentFactory extends MigrationComponentFactory {

	@Override
	public MigrationPlan newPredefinedMigrationPlan() {
//		return new TpccPredefinedMigrationPlan();
		// Debug
		ScatterMigrationPlan plan = new ScatterMigrationPlan();
		
		addWarehouse(plan, 0, 1, 2);
		addWarehouse(plan, 0, 2, 3);
		
		return plan;
	}
	
	public MigrationRange toMigrationRange(int sourceId, int destId, RecordKey partitioningKey) {
		TableKeyIterator keyIterator = null;
		int wid;
		boolean ignoreInsertion = false;
		
		switch (partitioningKey.getTableName()) {
		case "warehouse":
			wid = (Integer) partitioningKey.getVal("w_id").asJavaVal();
			keyIterator = new WarehouseKeyIterator(wid, 1);
			break;
		case "district":
			wid = (Integer) partitioningKey.getVal("d_w_id").asJavaVal();
			keyIterator = new DistrictKeyIterator(wid, 1);
			break;
		case "stock":
			wid = (Integer) partitioningKey.getVal("s_w_id").asJavaVal();
			keyIterator = new StockKeyIterator(wid, 1);
			break;
		case "customer":
			wid = (Integer) partitioningKey.getVal("c_w_id").asJavaVal();
			keyIterator = new CustomerKeyIterator(wid, 1);
			break;
		case "history":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("h_c_w_id").asJavaVal();
//			keyIterator = new HistoryKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("history");
			ignoreInsertion = true;
			break;
		case "orders":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("o_w_id").asJavaVal();
//			keyIterator = new OrdersKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("orders");
			ignoreInsertion = true;
			break;
		case "new_order":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("no_w_id").asJavaVal();
//			keyIterator = new NewOrderKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("new_order");
			ignoreInsertion = true;
			break;
		case "order_line":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("ol_w_id").asJavaVal();
//			keyIterator = new OrderLineKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("order_line");
			ignoreInsertion = true;
			break;
		default:
			return null;
		}
		
		return new SingleTableMigrationRange(sourceId, destId, partitioningKey, keyIterator, ignoreInsertion);
	}
	
	private void addWarehouse(ScatterMigrationPlan plan, int source, int dest, int wid) {
		
		plan.addKey(source, dest, newWarehouseKey(wid));
		
		for (int did = 1; did <= 10; did++) {
			plan.addKey(source, dest, newDistrictKey(wid, did));
			
			for (int cid = 1; cid <= 3000; cid++) {
				plan.addKey(source, dest, newCustomerKey(wid, did, cid));
			}
		}
		
		for (int iid = 1; iid <= 100000; iid++) {
			plan.addKey(source, dest, newStockKey(wid, iid));
		}
	}
	
	private RecordKey newWarehouseKey(int wid) {
		RecordKeyBuilder builder = new RecordKeyBuilder("warehouse");
		builder.addFldVal("w_id", new IntegerConstant(wid));
		return builder.build();
	}
	
	private RecordKey newDistrictKey(int d_w_id, int d_id) {
		RecordKeyBuilder builder = new RecordKeyBuilder("district");
		builder.addFldVal("d_w_id", new IntegerConstant(d_w_id));
		builder.addFldVal("d_id", new IntegerConstant(d_id));
		return builder.build();
	}
	
	private RecordKey newCustomerKey(int c_w_id, int c_d_id, int c_id) {
		RecordKeyBuilder builder = new RecordKeyBuilder("customer");
		builder.addFldVal("c_w_id", new IntegerConstant(c_w_id));
		builder.addFldVal("c_d_id", new IntegerConstant(c_d_id));
		builder.addFldVal("c_id", new IntegerConstant(c_id));
		return builder.build();
	}
	
	private RecordKey newStockKey(int wid, int iid) {
		RecordKeyBuilder builder = new RecordKeyBuilder("stock");
		builder.addFldVal("s_i_id", new IntegerConstant(iid));
		builder.addFldVal("s_w_id", new IntegerConstant(wid));
		return builder.build();
	}
}
