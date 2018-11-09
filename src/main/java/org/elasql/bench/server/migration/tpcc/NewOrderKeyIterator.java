package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.bench.server.procedure.calvin.tpcc.NewOrderProc;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class NewOrderKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int startWid, endWid;
	private int wid, did, oid;
	private int[][] maxOrderIds;
	
	private boolean hasNext = true;
	
	public NewOrderKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.startWid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		this.oid = TpccConstants.NEW_ORDER_START_ID;
		maxOrderIds = new int[wcount][10];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				maxOrderIds[wi][di] = NewOrderProc.getNextOrderId(wi + startWid, di + 1) - 1;
	}
	
	public NewOrderKeyIterator(NewOrderKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
		this.oid = iter.oid;
		this.startWid = iter.startWid;
		this.endWid = iter.endWid;
		this.hasNext = iter.hasNext;

		int wcount = endWid - startWid + 1;
		maxOrderIds = new int[wcount][10];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				maxOrderIds[wi][di] = iter.maxOrderIds[wi][di];
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public RecordKey next() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("no_w_id", new IntegerConstant(wid));
		keyEntryMap.put("no_d_id", new IntegerConstant(did));
		keyEntryMap.put("no_o_id", new IntegerConstant(oid));
		
		// move to the next
		oid++;
		if (oid > maxOrderIds[wid - startWid][did - 1]) {
			did++;
			oid = TpccConstants.NEW_ORDER_START_ID;
			
			if (did > 10) {
				wid++;
				did = 1;
				
				if (wid > endWid) {
					hasNext = false;
				}
			}
		}
		
		return new RecordKey("new_order", keyEntryMap);
	}

	@Override
	public String getTableName() {
		return "new_order";
	}

	@Override
	public boolean isInSubsequentKeys(RecordKey key) {
		if (!key.getTableName().equals("new_order"))
			return false;
		
		Integer keyWid = (Integer) key.getKeyVal("no_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
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
	}

}