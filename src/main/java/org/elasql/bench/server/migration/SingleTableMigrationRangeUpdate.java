package org.elasql.bench.server.migration;

import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.PrimaryKey;

public class SingleTableMigrationRangeUpdate implements MigrationRangeUpdate {
	
	private static final long serialVersionUID = 20181101001L;
	
	PrimaryKey partitioningKey;
	TableKeyIterator keyRangeToPush;
	int sourcePartId, destPartId;
	Set<PrimaryKey> otherMigratingKeys = new HashSet<PrimaryKey>();
	
	SingleTableMigrationRangeUpdate(int sourcePartId, int destPartId,
			PrimaryKey partitioningKey, TableKeyIterator keyRangeToPush, Set<PrimaryKey> otherMigratingKeys) {
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
