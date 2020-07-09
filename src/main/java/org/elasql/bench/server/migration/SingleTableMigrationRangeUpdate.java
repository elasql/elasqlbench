package org.elasql.bench.server.migration;

import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.RecordKey;

public class SingleTableMigrationRangeUpdate implements MigrationRangeUpdate {
	
	private static final long serialVersionUID = 20181101001L;
	
	RecordKey partitioningKey;
	TableKeyIterator keyRangeToPush;
	int sourcePartId, destPartId;
	Set<RecordKey> otherMigratingKeys = new HashSet<RecordKey>();
	
	SingleTableMigrationRangeUpdate(int sourcePartId, int destPartId,
			RecordKey partitioningKey, TableKeyIterator keyRangeToPush, Set<RecordKey> otherMigratingKeys) {
		this.partitioningKey = partitioningKey;
		this.keyRangeToPush = keyRangeToPush;
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
