package org.elasql.bench.server.migraion;

import java.util.Iterator;

import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationRange;
import org.elasql.sql.RecordKey;

public class TpccMigrationMgr extends MigrationMgr {

	@Override
	public Iterator<RecordKey> toKeyIterator(MigrationRange range) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public int toNumericId(RecordKey key) {
		throw new RuntimeException("Not implemented");
	}

}
