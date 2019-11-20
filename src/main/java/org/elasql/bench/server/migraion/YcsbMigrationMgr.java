package org.elasql.bench.server.migraion;

import java.util.Iterator;

import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationRange;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

public class YcsbMigrationMgr extends MigrationMgr {

	@Override
	public Iterator<RecordKey> toKeyIterator(MigrationRange range) {
		return new YcsbKeyIterator(range);
	}

	@Override
	public int toNumericId(RecordKey key) {
		// This only works for RangePartitionPlan
		Constant idCon = key.getKeyVal("ycsb_id");
		return Integer.parseInt((String) idCon.asJavaVal());
	}

}
