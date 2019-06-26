package org.elasql.bench.server.metadata;

import java.util.logging.Logger;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;


public class YcsbPartitionMetaMgr extends PartitionMetaMgr {
	private static Logger logger = Logger.getLogger(YcsbPartitionMetaMgr.class.getName());
	
	private static final String LOC_FILE_PATH = "/opt/shared/metis_ycsb_table.part";
	private static final int NUM_RECORDS = ElasqlYcsbConstants.RECORD_PER_PART * 4;
	private static final int RECORDS_ON_FIRST = NUM_RECORDS / 16 * 7;
	private static final int RECORDS_ON_OTHER = NUM_RECORDS / 16 * 3;
	
//	public static int getStartYcsbId(int vertexId) {
//		int higherPart = vertexId / YcsbMigrationManager.VERTEX_PER_PART; // 123 => 1
//		int lowerPart = vertexId % YcsbMigrationManager.VERTEX_PER_PART; // 123 => 23
//		return higherPart * ElasqlYcsbConstants.MAX_RECORD_PER_PART + lowerPart * YcsbMigrationManager.DATA_RANGE_SIZE; // 1 * 1000000000 + 23 * 10000
//	}
	
	public YcsbPartitionMetaMgr() {
		if (PartitionMetaMgr.USE_SCHISM) {
			//shoud load metis when loading testbed
			loadMetisPartitions();
			//monitor should commit it
		}
	}

	public void loadMetisPartitions() {
//		File file = new File(LOC_FILE_PATH);
//		if (!file.exists()) {
//			if (logger.isLoggable(Level.WARNING))
//				logger.warning(String.format("Cannot find Metis partitions at '%s'", LOC_FILE_PATH));
//			return;
//		}
//		
//		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//
//			String line;
//			Map<String, Constant> keyEntryMap;
//			int lineCount = 0;
//			while ((line = br.readLine()) != null) {
//				int newPartId = Integer.parseInt(line);
//				int startYcsbId = getStartYcsbId(lineCount);
//				
//				for (int i = 1; i <= MigrationManager.DATA_RANGE_SIZE; i++) {
//					keyEntryMap = new HashMap<String, Constant>();
//					keyEntryMap.put("ycsb_id", new VarcharConstant(
//							String.format(YcsbConstants.ID_FORMAT, startYcsbId + i)));
//					this.setPartition(new RecordKey("ycsb", keyEntryMap), newPartId);
//				}
//				lineCount++;
//			}
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}
	
	public static int getRangeIndex(RecordKey key) {
//		return getRangeIndex(Integer.parseInt(key.getKeyVal("ycsb_id").toString()));
		int id = Integer.parseInt(key.getKeyVal("ycsb_id").toString());
		return (id - 1) / ElasqlYcsbConstants.RECORD_PER_PART;
	}

	public static int getRangeIndex(int id) {
		return (id - 1) / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
	}
	
	public static int getIndexByHashPartition(RecordKey key) {
		Constant idCon = key.getKeyVal("ycsb_id");
		int id = Integer.parseInt((String) idCon.asJavaVal());
		return id % NUM_PARTITIONS;
	}

	public int skewedPartition(RecordKey key) {
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			int ycsbId = Integer.parseInt((String) idCon.asJavaVal());
			
			if (ycsbId <= RECORDS_ON_FIRST) {
				return 0;
			} else {
				return (ycsbId - RECORDS_ON_FIRST - 1) / RECORDS_ON_OTHER + 1;
			}
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}

	public int getLocation(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */

		// For a special type of record

		if (key.getTableName().equals("notification"))
			return -1;

		Constant ycsbIdCon = key.getKeyVal("ycsb_id");

		if (ycsbIdCon == null) {
			// Fully replicated
			return Elasql.serverId();
		}
		
		// Range-partition
		return getRangeIndex(key);
		// Hash-partition
//		return getIndexByHashPartition(key);
		// Skewed-partition
//		return skewedPartition(key);
	}
}
