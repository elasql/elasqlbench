package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class WarehouseKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid;
	
	private boolean hasNext = true;
	
	public WarehouseKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
	}
	
	public WarehouseKeyIterator(WarehouseKeyIterator iter) {
		this.wid = iter.wid;
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
		keyEntryMap.put("w_id", new IntegerConstant(wid));
		
		// move to the next
		wid++;
		if (wid > endWid) {
			hasNext = false;
		}
		
		return new RecordKey("warehouse", keyEntryMap);
	}

	@Override
	public String getTableName() {
		return "warehouse";
	}

	@Override
	public boolean isInSubsequentKeys(RecordKey key) {
		if (!key.getTableName().equals("warehouse"))
			return false;
		
		Integer keyWid = (Integer) key.getKeyVal("w_id").asJavaVal();
		return keyWid >= wid && keyWid <= endWid;
	}

}
