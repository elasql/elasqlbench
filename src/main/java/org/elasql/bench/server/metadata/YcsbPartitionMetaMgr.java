package org.elasql.bench.server.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;

public class YcsbPartitionMetaMgr extends PartitionMetaMgr {
private static Logger logger = Logger.getLogger(YcsbPartitionMetaMgr.class.getName());
	
	private static final String LOC_FILE_PATH = "/opt/shared/metis-partitions/google-20/mon30s-iter60s-ran10/tail.part";
	
	private static final int VERTEX_PER_PART = ElasqlYcsbConstants.RECORD_PER_PART / METIS_DATA_RANGE;
	
	private Map<Integer, Integer> schismMap = new HashMap<Integer, Integer>();
	
	public YcsbPartitionMetaMgr() {
		if (LOAD_METIS_PARTITIONS) {
			//shoud load metis when loading testbed
			loadMetisPartitions();
			//monitor should commit it
			
			if (logger.isLoggable(Level.INFO))
				logger.info("finish loading Metis' partition plan");
		}
	}

	public void loadMetisPartitions() {
		File file = new File(LOC_FILE_PATH);
		if (!file.exists()) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(String.format("Cannot find Metis partitions at '%s'", LOC_FILE_PATH));
			return;
		}
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			String line;
//			Map<String, Constant> keyEntryMap;
			int lineCount = 0;
			while ((line = br.readLine()) != null) {
				int newPartId = Integer.parseInt(line);
				schismMap.put(lineCount, newPartId);
				
//				int higherPart = lineCount / VERTEX_PER_PART; // 123 => 1
//				int lowerPart = lineCount % VERTEX_PER_PART; // 123 => 23
//				int startYcsbId = higherPart * ElasqlYcsbConstants.MAX_RECORD_PER_PART + lowerPart * METIS_DATA_RANGE; // 1 * 1000000000 + 23 * 10000
//				
//				for (int i = 1; i <= METIS_DATA_RANGE; i++) {
//					keyEntryMap = new HashMap<String, Constant>();
//					keyEntryMap.put("ycsb_id", new VarcharConstant(
//							String.format(YcsbConstants.ID_FORMAT, startYcsbId + i)));
//					setCurrentLocation(new RecordKey("ycsb", keyEntryMap), newPartId);
//				}
				
				lineCount++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}
	
	public int convertToVertexId(int ycsbId)
	{
		ycsbId -= 1; // [1, N] => [0, N-1]
		int partId = ycsbId / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
		int vertexIdInPart = (ycsbId % ElasqlYcsbConstants.MAX_RECORD_PER_PART) / METIS_DATA_RANGE;
		return partId * VERTEX_PER_PART + vertexIdInPart;
	}

	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Ycsb-benchmark testbed.
		 * Partitions each item id through mod.
		 */
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			int ycsbId = Integer.parseInt((String) idCon.asJavaVal());
			
			if (LOAD_METIS_PARTITIONS) {
				// Hash-based
//				return ycsbId % NUM_PARTITIONS;
				
				// Range-based (shift)
//				int partId = ycsbId / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
//				int index = ycsbId % ElasqlYcsbConstants.MAX_RECORD_PER_PART;
//				if (index > 800000)
//					return (partId + 1) % NUM_PARTITIONS;
//				else
//					return partId;
				
				// Schism-based
				int vid = convertToVertexId(ycsbId);
				return schismMap.get(vid);
			}
			
			// Range-based
			return ycsbId / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
}
