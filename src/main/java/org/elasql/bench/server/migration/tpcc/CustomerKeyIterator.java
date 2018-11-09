package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class CustomerKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid, did, cid;
	
	private boolean hasNext = true;
	
	public CustomerKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		this.cid = 1;
	}
	
	public CustomerKeyIterator(CustomerKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
		this.cid = iter.cid;
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
					hasNext = false;
				}
			}
		}
		
		return new RecordKey("customer", keyEntryMap);
	}

	@Override
	public String getTableName() {
		return "customer";
	}

	@Override
	public boolean isInSubsequentKeys(RecordKey key) {
		if (!key.getTableName().equals("customer"))
			return false;
		
		Integer keyWid = (Integer) key.getKeyVal("c_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
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
	}

}
