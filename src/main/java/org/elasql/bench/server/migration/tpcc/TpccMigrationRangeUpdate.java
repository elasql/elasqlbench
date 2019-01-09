package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.RecordKey;

public class TpccMigrationRangeUpdate implements MigrationRangeUpdate, Serializable {
	
	private static final long serialVersionUID = 20181101001L;
	
	int minWid;
	TpccKeyIterator unmigratedKeys;
	int sourcePartId, destPartId;
	Set<RecordKey> otherMigratingKeys = new HashSet<RecordKey>();
	
	TpccMigrationRangeUpdate(int sourcePartId, int destPartId,
			int minWid, TpccKeyIterator unmigratedKeys, Set<RecordKey> otherMigratingKeys) {
		this.minWid = minWid;
		this.unmigratedKeys = unmigratedKeys;
		this.otherMigratingKeys = otherMigratingKeys;
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
	}
	
	@Override
	public int getSourcePartId() {
		return sourcePartId;
	}

	@Override
	public int getDestPartId() {
		return destPartId;
	}
}
