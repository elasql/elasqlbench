package org.elasql.bench.server.migraion;

import java.util.HashMap;
import java.util.Iterator;

import org.elasql.migration.MigrationRange;
import org.elasql.sql.RecordKey;
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class YcsbKeyIterator implements Iterator<RecordKey> {
	
	private String table;
	private String keyField;
	private int nextId;
	private int endId;
	
	public YcsbKeyIterator(MigrationRange range) {
		this.table = range.getTableName();
		this.keyField = range.getKeyFieldName();
		this.nextId = range.getStartId();
		this.endId = range.getEndId();
	}

	@Override
	public boolean hasNext() {
		return nextId <= endId;
	}

	@Override
	public RecordKey next() {
		HashMap<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put(keyField, new VarcharConstant(
				String.format(YcsbConstants.ID_FORMAT, nextId)));
		RecordKey key = new RecordKey(table, fldVals);
		nextId++;
		return key;
	}
	
	
}
