package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.bench.server.procedure.calvin.tpcc.NewOrderProc;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class OrderLineKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int startWid, endWid;
	private int wid, did, oid, olnum;
	private int[][] maxOrderIds;
	
	private boolean hasNext = true;
	
	public OrderLineKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.startWid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		this.oid = 1;
		maxOrderIds = new int[wcount][10];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				maxOrderIds[wi][di] = NewOrderProc.getNextOrderId(wi + startWid, di + 1) - 1;
	}
	
	public OrderLineKeyIterator(OrderLineKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
		this.oid = iter.oid;
		this.olnum = iter.olnum;
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
		keyEntryMap.put("ol_w_id", new IntegerConstant(wid));
		keyEntryMap.put("ol_d_id", new IntegerConstant(did));
		keyEntryMap.put("ol_o_id", new IntegerConstant(oid));
		keyEntryMap.put("ol_number", new IntegerConstant(olnum));
		
		// move to the next
		olnum++;
		
		if (olnum > 10) {
			oid++;
			olnum = 1;
		
			if (oid > maxOrderIds[wid - startWid][did - 1]) {
				did++;
				oid = 1;
				
				if (did > 10) {
					wid++;
					did = 1;
					
					if (wid > endWid) {
						hasNext = false;
					}
				}
			}
		}
		
		return new RecordKey("order_line", keyEntryMap);
	}

	@Override
	public String getTableName() {
		return "order_line";
	}

	@Override
	public boolean isInSubsequentKeys(RecordKey key) {
		if (!key.getTableName().equals("order_line"))
			return false;
		
		Integer keyWid = (Integer) key.getKeyVal("ol_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
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
	}

}
