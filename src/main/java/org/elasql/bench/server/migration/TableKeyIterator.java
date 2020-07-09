package org.elasql.bench.server.migration;

import java.util.Iterator;

import org.elasql.sql.RecordKey;

public interface TableKeyIterator extends Iterator<RecordKey> {
	
	String getTableName();
	
	boolean isInSubsequentKeys(RecordKey key);
	
	TableKeyIterator copy();
	
}
