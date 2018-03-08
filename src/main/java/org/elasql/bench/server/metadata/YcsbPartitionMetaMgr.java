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
import org.vanilladb.bench.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class YcsbPartitionMetaMgr extends PartitionMetaMgr {
private static Logger logger = Logger.getLogger(YcsbPartitionMetaMgr.class.getName());
	
	private static final String LOC_FILE_PATH = "/opt/shared/metis_ycsb_table.part";
	
	private static final int VERTEX_PER_PART = ElasqlYcsbConstants.RECORD_PER_PART / METIS_DATA_RANGE;
	
	public YcsbPartitionMetaMgr() {
		if (LOAD_METIS_PARTITIONS) {
			//shoud load metis when loading testbed
			loadMetisPartitions();
			//monitor should commit it
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Successfully loaded the Metis partitions");
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
			Map<String, Constant> keyEntryMap;
			int lineCount = 0;
			while ((line = br.readLine()) != null) {
				int newPartId = Integer.parseInt(line);
				int higherPart = lineCount / VERTEX_PER_PART; // 123 => 1
				int lowerPart = lineCount % VERTEX_PER_PART; // 123 => 23
				int startYcsbId = higherPart * ElasqlYcsbConstants.MAX_RECORD_PER_PART + lowerPart * METIS_DATA_RANGE; // 1 * 1000000000 + 23 * 10000
				
				for (int i = 1; i <= METIS_DATA_RANGE; i++) {
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("ycsb_id", new VarcharConstant(
							String.format(YcsbConstants.ID_FORMAT, startYcsbId + i)));
					setCurrentLocation(new RecordKey("ycsb", keyEntryMap), newPartId);
				}
				lineCount++;

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Ycsb-benchmark testbed.
		 * Partitions each item id through mod.
		 */
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			String id = (String) idCon.asJavaVal();
			return (Integer.parseInt(id) - 1) / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
}
