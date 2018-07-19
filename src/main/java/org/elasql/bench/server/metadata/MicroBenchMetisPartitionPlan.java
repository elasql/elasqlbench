package org.elasql.bench.server.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class MicroBenchMetisPartitionPlan extends PartitionPlan {
	private static Logger logger = Logger.getLogger(MicroBenchMetisPartitionPlan.class.getName());

	private static final int METIS_DATA_RANGE = 1;
	
	private PartitionPlan underlayerPlan;
	private Map<RecordKey, Integer> metisPlan = new HashMap<RecordKey, Integer>();
	
	public MicroBenchMetisPartitionPlan(PartitionPlan defaultPlan, String metisFilePath) {
		underlayerPlan = defaultPlan;
		
		//shoud load metis when loading testbed
		loadMetisPartitions(metisFilePath);
		//monitor should commit it
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Successfully loaded the Metis partitions");
	}

	public void loadMetisPartitions(String metisFilePath) {
		File file = new File(metisFilePath);
		if (!file.exists()) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(String.format("Cannot find Metis partitions at '%s'", metisFilePath));
			return;
		}
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			String sCurrentLine;
			Map<String, Constant> keyEntryMap;
			int line_c = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				for (int i = 1; i <= METIS_DATA_RANGE; i++) {
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("i_id", new IntegerConstant(METIS_DATA_RANGE * line_c + i));
					metisPlan.put(new RecordKey("item", keyEntryMap), Integer.parseInt(sCurrentLine));
				}
				line_c++;

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean isFullyReplicated(RecordKey key) {
		return underlayerPlan.isFullyReplicated(key);
	}


	public int getPartition(RecordKey key) {
		// Check the metis first
		Integer id = metisPlan.get(key);
		
		if (id != null)
			return id;
			
		// If not found, check the underlayer plan
		return underlayerPlan.getPartition(key);
	}
}
