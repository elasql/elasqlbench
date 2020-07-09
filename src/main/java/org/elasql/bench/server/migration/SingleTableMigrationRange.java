package org.elasql.bench.server.migration;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class SingleTableMigrationRange implements MigrationRange {
	
	// Partitioning key
	private RecordKey partitioningKey;
	private int sourcePartId, destPartId;
	
	private TableKeyIterator keyRangeToPush;
	private TableKeyIterator chunkGenerator;
	
	// For new inserted keys
	private Set<RecordKey> unmigratedNewKeys = new HashSet<RecordKey>();
	private ConcurrentLinkedQueue<RecordKey> nextMigratingNewKeys =
			new ConcurrentLinkedQueue<RecordKey>();
	private Set<RecordKey> newKeysInRecentChunk = new HashSet<RecordKey>();
	private boolean ignoreInsertion;
	
	// We does not remove the contents until the entire migration finishes
	private Set<RecordKey> migratedKeys = new HashSet<RecordKey>();
	
	// Note: this can only be called from the scheduler
	public SingleTableMigrationRange(int sourcePartId, int destPartId, RecordKey partitioningKey,
			TableKeyIterator keyIterator, boolean ignoreInsertion) {
		this.partitioningKey = partitioningKey;
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
		this.keyRangeToPush = keyIterator.copy();
		this.chunkGenerator = keyIterator.copy();
		this.ignoreInsertion = ignoreInsertion;
	}
	
	@Override
	public boolean addKey(RecordKey key) {
		if (!contains(key))
			return false;
		
		if (ignoreInsertion)
			return true;
		
		unmigratedNewKeys.add(key);
		nextMigratingNewKeys.add(key);
		return true;
	}

	@Override
	public boolean contains(RecordKey key) {
		RecordKey partKey = Elasql.partitionMetaMgr().getPartitioningKey(key);
		return partitioningKey.equals(partKey);
	}
	
	public boolean isMigrated(RecordKey key) {
		if (!migratedKeys.contains(key)) {
			if (unmigratedNewKeys.contains(key))
				return false;
			
			return !keyRangeToPush.isInSubsequentKeys(key);
		}
		return true;
	}
	
	public void setMigrated(RecordKey key) {
		if (unmigratedNewKeys.remove(key))
			return;
		
		if (keyRangeToPush.isInSubsequentKeys(key))
			migratedKeys.add(key);
	}
	
	// This may be called by another thread on the destination node
	/**
	 * If 'useBytesForSize' is enabled, it will use the bytes to represent the chunk size. If not,
	 * it will use the number of records. 
	 */
	public Set<RecordKey> generateNextMigrationChunk(boolean useBytesForSize, int maxChunkSize) {
		Set<RecordKey> chunk = new HashSet<RecordKey>();
		int chunkSize = 0;
		
		// Migrate the new inserted keys
		while (!nextMigratingNewKeys.isEmpty() && chunkSize < maxChunkSize) {
			RecordKey key = nextMigratingNewKeys.poll();
			
			// It is Ok that we do not check if the new key is migrated
			// because if it is migrated, we will prevent it from inserting.
			
			if (useBytesForSize)
				chunkSize += recordSize(key.getTableName());
			else
				chunkSize++;
			
			chunk.add(key);
			newKeysInRecentChunk.add(key);
		}
		
		// Migrate the other existing keys
		while (chunkGenerator.hasNext() && chunkSize < maxChunkSize) {
			RecordKey key = chunkGenerator.next();
			
			if (useBytesForSize)
				chunkSize += recordSize(key.getTableName());
			else
				chunkSize++;
			
			chunk.add(key);
		}
		
		return chunk;
	}
	
	/**
	 * MigrationRangeUpdate is used by background pushes to update the migration
	 * range in a single action. If we did not use this manner, it would require
	 * to record which keys are migrated. This might create large memory overhead.
	 * 
	 * @return
	 */
	@Override
	public MigrationRangeUpdate generateStatusUpdate() {
		return new SingleTableMigrationRangeUpdate(sourcePartId, destPartId,
				partitioningKey, chunkGenerator.copy(), newKeysInRecentChunk);
	}

	@Override
	public boolean updateMigrationStatus(MigrationRangeUpdate update) {
		SingleTableMigrationRangeUpdate su = (SingleTableMigrationRangeUpdate) update;
		if (su.partitioningKey.equals(partitioningKey)) {
			keyRangeToPush = su.keyRangeToPush;
			for (RecordKey key : su.otherMigratingKeys)
				setMigrated(key);
			return true;
		} else
			return false;
	}

	@Override
	public int getSourcePartId() {
		return sourcePartId;
	}

	@Override
	public int getDestPartId() {
		return destPartId;
	}
	
	private int recordSize(String tableName){
		switch(tableName){
		case "warehouse":
			return 344;
		case "district":
			return 352;
		case "customer":
			return 2552;
		case "history":
			return 132;
		case "new_order":
			return 12;
		case "orders":
			return 36;
		case "order_line":
			return 140;
		case "item":
			return 320;
		case "stock":
			return 1184;
		default:
			throw new IllegalArgumentException("No such table for TPCC");
		}
	}
	
	@Override
	public String toString() {
		return String.format("[partitioning key: %s, from node %d to node %d]", 
				partitioningKey, sourcePartId, destPartId);
	}
}
