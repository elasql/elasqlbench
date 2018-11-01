package org.elasql.bench.server.metadata.migration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.bench.server.procedure.calvin.tpcc.NewOrderProc;
import org.elasql.bench.server.procedure.calvin.tpcc.PaymentProc;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class TpccKeyIterator implements Iterator<RecordKey> {
	
	private enum Table {
		WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, NEW_ORDER, ORDERS, ORDER_LINE, STOCK, NONE;
		
		static Table parse(String tableName) {
			switch (tableName) {
			case "warehouse":
				return WAREHOUSE;
			case "district":
				return DISTRICT;
			case "customer":
				return CUSTOMER;
			case "history":
				return HISTORY;
			case "new_order":
				return NEW_ORDER;
			case "orders":
				return ORDERS;
			case "order_line":
				return ORDER_LINE;
			case "stock":
				return STOCK;
			default:
				return NONE;
			}
		}
		
		int compareTo(String tableName) {
			Table table = parse(tableName);
			return this.ordinal() - table.ordinal();
		}
	};
	
	private Table currentTable;
	private int wid, did, cid, hid, oid, olnum, iid;
	
	// start and end are inclusive
	private int startWid, endWid;
	private int[][] maxOrderIds;
	private int[][][] maxHistoryIds;
	
	// Note: this can be only called from the scheduler.
	public TpccKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.startWid = startWid;
		this.endWid = startWid + wcount - 1;
		this.currentTable = Table.WAREHOUSE;
		maxOrderIds = new int[wcount][10];
		maxHistoryIds = new int[wcount][10][3000];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++) {
				maxOrderIds[wi][di] = NewOrderProc.getNextOrderId(wi + startWid, di + 1) - 1;
				for (int ci = 0; ci < 3000; ci++)
					maxHistoryIds[wi][di][ci] = PaymentProc.getNextHistoryId(wi + startWid, di + 1, ci + 1) - 1;
			}
	}
	
	public boolean hasNext() {
		return currentTable != Table.NONE;
	}
	
	public RecordKey next() {
		switch (currentTable) {
		case WAREHOUSE:
			return nextWarehouseKey();
		case DISTRICT:
			return nextDistrictKey();
		case CUSTOMER:
			return nextCustomerKey();
		case HISTORY:
			return nextHistoryKey();
		case NEW_ORDER:
			return nextNewOrderKey();
		case ORDERS:
			return nextOrdersKey();
		case ORDER_LINE:
			return nextOrderLineKey();
		case STOCK:
			return nextStockKey();
		default:
			return null;
		}
	}
	
	public boolean isInSubsequentKeys(RecordKey key) {
		Integer keyWid = TpccPartitionPlan.getWarehouseId(key);
		if (keyWid == null || (startWid > keyWid && keyWid > endWid))
			return false;
		
		int compare = currentTable.compareTo(key.getTableName());
		if (compare < 0)
			return true;
		else if (compare > 0)
			return false;
		
		switch (Table.parse(key.getTableName())) {
		case WAREHOUSE:
			return keyWid >= wid;
		case DISTRICT:
			if (keyWid > wid)
				return true;
			else if (keyWid < wid)
				return false;
			else {
				Integer keyDid = (Integer) key.getKeyVal("d_id").asJavaVal();
				return keyDid >= did;
			}
		case CUSTOMER:
			if (keyWid > wid)
				return true;
			else if (keyWid < wid)
				return false;
			else {
				Integer keyDid = (Integer) key.getKeyVal("c_d_id").asJavaVal();
				if (keyDid > did)
					return true;
				else if (keyDid < did)
					return false;
				else {
					Integer keyCid = (Integer) key.getKeyVal("c_id").asJavaVal();
					return keyCid >= cid;
				}
			}
		case HISTORY:
			if (keyWid > wid)
				return true;
			else if (keyWid < wid)
				return false;
			else {
				Integer keyDid = (Integer) key.getKeyVal("h_c_d_id").asJavaVal();
				if (keyDid > did)
					return true;
				else if (keyDid < did)
					return false;
				else {
					Integer keyCid = (Integer) key.getKeyVal("h_c_id").asJavaVal();
					if (keyCid > cid)
						return true;
					else if (keyCid < cid)
						return false;
					else {
						Integer keyHid = (Integer) key.getKeyVal("h_id").asJavaVal();
						return keyHid >= hid;
					}
				}
			}
		case NEW_ORDER:
			if (keyWid > wid)
				return true;
			else if (keyWid < wid)
				return false;
			else {
				Integer keyDid = (Integer) key.getKeyVal("no_d_id").asJavaVal();
				if (keyDid > did)
					return true;
				else if (keyDid < did)
					return false;
				else {
					Integer keyOid = (Integer) key.getKeyVal("no_o_id").asJavaVal();
					return keyOid >= oid;
				}
			}
		case ORDERS:
			if (keyWid > wid)
				return true;
			else if (keyWid < wid)
				return false;
			else {
				Integer keyDid = (Integer) key.getKeyVal("o_d_id").asJavaVal();
				if (keyDid > did)
					return true;
				else if (keyDid < did)
					return false;
				else {
					Integer keyOid = (Integer) key.getKeyVal("o_id").asJavaVal();
					return keyOid >= oid;
				}
			}
		case ORDER_LINE:
			if (keyWid > wid)
				return true;
			else if (keyWid < wid)
				return false;
			else {
				Integer keyDid = (Integer) key.getKeyVal("ol_d_id").asJavaVal();
				if (keyDid > did)
					return true;
				else if (keyDid < did)
					return false;
				else {
					Integer keyOid = (Integer) key.getKeyVal("ol_o_id").asJavaVal();
					if (keyOid > oid)
						return true;
					else if (keyOid < oid)
						return false;
					else {
						Integer keyOlnum = (Integer) key.getKeyVal("ol_number").asJavaVal();
						return keyOlnum >= olnum;
					}
				}
			}
		case STOCK:
			if (keyWid > wid)
				return true;
			else if (keyWid < wid)
				return false;
			else {
				Integer keyIid = (Integer) key.getKeyVal("s_i_id").asJavaVal();
				return keyIid >= iid;
			}
		default:
			return false;
		}
	}
	
	private RecordKey nextWarehouseKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("w_id", new IntegerConstant(wid));
		
		// move to the next
		wid++;
		if (wid > endWid) {
			currentTable = Table.DISTRICT;
			wid = startWid;
			did = 1;
		}
		
		return new RecordKey("warehouse", keyEntryMap);
	}
	
	private RecordKey nextDistrictKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("d_w_id", new IntegerConstant(wid));
		keyEntryMap.put("d_id", new IntegerConstant(did));
		
		// move to the next
		did++;
		if (did > 10) {
			wid++;
			did = 1;
			
			if (wid > endWid) {
				currentTable = Table.CUSTOMER;
				wid = startWid;
				did = 1;
				cid = 1;
			}
		}
		
		return new RecordKey("district", keyEntryMap);
	}
	
	private RecordKey nextCustomerKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("c_w_id", new IntegerConstant(wid));
		keyEntryMap.put("c_d_id", new IntegerConstant(did));
		keyEntryMap.put("c_id", new IntegerConstant(cid));
		
		// move to the next
		cid++;
		if (cid > 3000) {
			did++;
			cid = 1;
			
			if (did > 10) {
				wid++;
				did = 1;
				
				if (wid > endWid) {
					currentTable = Table.HISTORY;
					wid = startWid;
					did = 1;
					cid = 1;
					hid = 1;
				}
			}
		}
		
		return new RecordKey("customer", keyEntryMap);
	}
	
	private RecordKey nextHistoryKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("h_id", new IntegerConstant(hid));
		keyEntryMap.put("h_c_w_id", new IntegerConstant(wid));
		keyEntryMap.put("h_c_d_id", new IntegerConstant(did));
		keyEntryMap.put("h_c_id", new IntegerConstant(cid));
		
		// move to the next
		hid++;
		if (hid > maxHistoryIds[wid][did][cid]) {
			cid++;
			hid = 1;
			
			if (cid > 3000) {
				did++;
				cid = 1;
				
				if (did > 10) {
					wid++;
					did = 1;
					
					if (wid > endWid) {
						currentTable = Table.NEW_ORDER;
						wid = startWid;
						did = 1;
						oid = TpccConstants.NEW_ORDER_START_ID;
					}
				}
			}
		}
		
		return new RecordKey("history", keyEntryMap);
	}
	
	private RecordKey nextNewOrderKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("no_w_id", new IntegerConstant(wid));
		keyEntryMap.put("no_d_id", new IntegerConstant(did));
		keyEntryMap.put("no_o_id", new IntegerConstant(oid));
		
		// move to the next
		oid++;
		if (oid > maxOrderIds[wid][did]) {
			did++;
			oid = TpccConstants.NEW_ORDER_START_ID;
			
			if (did > 10) {
				wid++;
				did = 1;
				
				if (wid > endWid) {
					currentTable = Table.ORDERS;
					wid = startWid;
					did = 1;
					oid = 1;
				}
			}
		}
		
		return new RecordKey("new_order", keyEntryMap);
	}
	
	private RecordKey nextOrdersKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("o_w_id", new IntegerConstant(wid));
		keyEntryMap.put("o_d_id", new IntegerConstant(did));
		keyEntryMap.put("o_id", new IntegerConstant(oid));
		
		// move to the next
		oid++;
		if (oid > maxOrderIds[wid][did]) {
			did++;
			oid = 1;
			
			if (did > 10) {
				wid++;
				did = 1;
				
				if (wid > endWid) {
					currentTable = Table.ORDER_LINE;
					wid = startWid;
					did = 1;
					oid = 1;
				}
			}
		}
		
		return new RecordKey("orders", keyEntryMap);
	}
	
	private RecordKey nextOrderLineKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ol_w_id", new IntegerConstant(wid));
		keyEntryMap.put("ol_d_id", new IntegerConstant(did));
		keyEntryMap.put("ol_o_id", new IntegerConstant(oid));
		keyEntryMap.put("ol_number", new IntegerConstant(olnum));
		
		// move to the next
		olnum++;
		
		if (olnum > 10) {
			oid++;
			olnum = 1;
		
			if (oid > maxOrderIds[wid][did]) {
				did++;
				oid = 1;
				
				if (did > 10) {
					wid++;
					did = 1;
					
					if (wid > endWid) {
						currentTable = Table.STOCK;
						wid = startWid;
						iid = 1;
					}
				}
			}
		}
		
		return new RecordKey("order_line", keyEntryMap);
	}
	
	private RecordKey nextStockKey() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("s_i_id", new IntegerConstant(iid));
		keyEntryMap.put("s_w_id", new IntegerConstant(wid));
		
		// move to the next
		iid++;
		if (iid > 100000) {
			wid++;
			iid = 1;
			
			if (wid > endWid) {
				currentTable = Table.NONE;
			}
		}
		
		return new RecordKey("stock", keyEntryMap);
	}
}
