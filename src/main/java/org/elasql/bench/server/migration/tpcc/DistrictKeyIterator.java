package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class DistrictKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid, did;
	
	private boolean hasNext = true;
	
	public DistrictKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
	}
	
	public DistrictKeyIterator(DistrictKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
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
		keyEntryMap.put("d_w_id", new IntegerConstant(wid));
		keyEntryMap.put("d_id", new IntegerConstant(did));
		
		// move to the next
		did++;
		if (did > 10) {
			wid++;
			did = 1;
			
			if (wid > endWid) {
				hasNext = false;
			}
		}
		
		return new RecordKey("district", keyEntryMap);
	}

	@Override
	public String getTableName() {
		return "district";
	}

	@Override
	public boolean isInSubsequentKeys(RecordKey key) {
		if (!key.getTableName().equals("district"))
			return false;
		
		Integer keyWid = (Integer) key.getKeyVal("d_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyDid = (Integer) key.getKeyVal("d_id").asJavaVal();
			return keyDid >= did;
		}
	}

}
