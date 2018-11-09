package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class StockKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid, iid;
	
	private boolean hasNext = true;
	
	public StockKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
		this.iid = 1;
	}
	
	public StockKeyIterator(StockKeyIterator iter) {
		this.wid = iter.wid;
		this.iid = iter.iid;
		this.endWid = iter.endWid;
		this.hasNext = iter.hasNext;
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public RecordKey next() {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("s_i_id", new IntegerConstant(iid));
		keyEntryMap.put("s_w_id", new IntegerConstant(wid));
		
		// move to the next
		iid++;
		if (iid > 100000) {
			wid++;
			iid = 1;
			
			if (wid > endWid) {
				hasNext = false;
			}
		}
		
		return new RecordKey("stock", keyEntryMap);
	}

	@Override
	public String getTableName() {
		return "stock";
	}

	@Override
	public boolean isInSubsequentKeys(RecordKey key) {
		if (!key.getTableName().equals("stock"))
			return false;
		
		Integer keyWid = (Integer) key.getKeyVal("s_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyIid = (Integer) key.getKeyVal("s_i_id").asJavaVal();
			return keyIid >= iid;
		}
	}

}
