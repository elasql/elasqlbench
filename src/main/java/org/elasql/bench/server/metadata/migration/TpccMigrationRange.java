package org.elasql.bench.server.metadata.migration;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.sql.RecordKey;
import org.elasql.util.PeriodicalJob;

public class TpccMigrationRange implements MigrationRange {
	
	// Partitioning key - warehouse id (wid)
	private int minWid, maxWid;
	
	private int sourcePartId, destPartId;
	
	private TpccKeyIterator keyIter;
	private Set<RecordKey> migratedKeys = new HashSet<RecordKey>();
	private AtomicInteger migratedCounts = new AtomicInteger(0);
	
	// Note: this can only be called from the scheduler
	public TpccMigrationRange(int minWid, int maxWid, int sourcePartId, int destPartId) {
		this.minWid = minWid;
		this.maxWid = maxWid;
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
		this.keyIter = new TpccKeyIterator(minWid, maxWid - minWid + 1);

		new PeriodicalJob(3000, 500000, new Runnable() {
			@Override
			public void run() {
				System.out.println("" + migratedCounts.get() + " has been migrated for warehouse " + minWid);
			}
		}).start();
	}

	@Override
	public boolean contains(RecordKey key) {
		int wid = TpccPartitionPlan.getWarehouseId(key);
		return minWid <= wid && wid <= maxWid;
	}
	
	public boolean isMigrated(RecordKey key) {
		if (!migratedKeys.contains(key))
			return !keyIter.isInSubsequentKeys(key);
		return true;
	}
	
	public void setMigrated(RecordKey key) {
		if (keyIter.isInSubsequentKeys(key)) {
			if (!migratedKeys.contains(key))
				migratedCounts.incrementAndGet();
			migratedKeys.add(key);
		}
	}
	
	public Set<RecordKey> generateNextMigrationChunk(int maxChunkSize) {
		Set<RecordKey> chunk = new HashSet<RecordKey>();
		int chunkSize = 0;
		
		while (keyIter.hasNext() && chunkSize < maxChunkSize) {
			RecordKey key = keyIter.next();
			chunkSize += recordSize(key.getTableName());
			chunk.add(key);
			migratedKeys.remove(key);
		}
		
		return chunk;
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
		return String.format("[warehouse: %d ~ %d, from node %d to node %d]", 
				minWid, maxWid, sourcePartId, destPartId);
	}
}
