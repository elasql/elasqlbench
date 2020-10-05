package org.elasql.bench.server.metadata.ycsb;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;

// XXX: Need to be checked before using
public class YcsbMetisPartitionPlan implements PartitionPlan {
	private static Logger logger = Logger.getLogger(YcsbMetisPartitionPlan.class.getName());
	
	private static final long serialVersionUID = 1L;
	
	private PartitionPlan underlayerPlan;
	private Map<RecordKey, Integer> metisPlan = new HashMap<RecordKey, Integer>();
	private String metisDirPath;
	
	public YcsbMetisPartitionPlan(PartitionPlan defaultPlan, String metisDirPath) {
		this.underlayerPlan = defaultPlan;
		this.metisDirPath = metisDirPath;
		
		//shoud load metis when loading testbed
		loadMetisPartitions(metisDirPath);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Successfully loaded the Metis partitions (" + metisPlan.size() + " records loaded)");
	}

	private void loadMetisPartitions(String metisDirPath) {
		File file = new File(metisDirPath);
		if (!file.exists())
			throw new RuntimeException(String.format("Cannot find anything at '%s'", metisDirPath));
		if (!file.isDirectory())
			throw new RuntimeException(String.format("'%s' is not a directory", metisDirPath));
		
		
		try {

			// Read the mapping file
			File mappingFile = new File(metisDirPath, "mapping.bin");
			Map<Integer, RecordKey> mapping = loadMappingFile(mappingFile);
			
			// Read the metis partition file
			File metisFile = new File(metisDirPath, "metis.part");
			metisPlan = loadPartitionFile(metisFile, mapping);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Map<Integer, RecordKey> loadMappingFile(File mappingFilePath) throws IOException {
		Map<Integer, RecordKey> idToKeys = null;
		try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(
				new FileInputStream(mappingFilePath)))) {
			try {
				Map<RecordKey, Integer> keyToIds = (Map<RecordKey, Integer>) in.readObject();
				idToKeys = new HashMap<Integer, RecordKey>();
				for (Map.Entry<RecordKey, Integer> entry : keyToIds.entrySet()) {
					idToKeys.put(Integer.valueOf(entry.getValue()), entry.getKey());
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Cannot read the mapping file from '" + mappingFilePath + "'");
			}
		}
		return idToKeys;
	}
	
	private Map<RecordKey, Integer> loadPartitionFile(File partitionFilePath,
			Map<Integer, RecordKey> idToKeys) throws IOException {
		Map<RecordKey, Integer> partitions = new HashMap<RecordKey, Integer>();
		
		try (BufferedReader reader = new BufferedReader(new FileReader(partitionFilePath))) {
			String line = reader.readLine();
			int id = 1;
			while (line != null) {
				Integer partId = Integer.valueOf(Integer.parseInt(line.trim()));
				RecordKey key = idToKeys.get(Integer.valueOf(id));
				partitions.put(key, partId);
				
				// Next line
				id++;
				line = reader.readLine();
			}
		}
		
		return partitions;
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

	@Override
	public PartitionPlan getBasePartitionPlan() {
		return underlayerPlan.getBasePartitionPlan();
	}

	@Override
	public boolean isBasePartitionPlan() {
		return false;
	}

	@Override
	public void changeBasePartitionPlan(PartitionPlan plan) {
		if (underlayerPlan.isBasePartitionPlan()) {
			underlayerPlan = plan;
		} else {
			underlayerPlan.changeBasePartitionPlan(plan);
		}
	}

	@Override
	public int numberOfPartitions() {
		return PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	@Override
	public String toString() {
		return String.format("MetisPartition (partition file: %s): [%s]", metisDirPath, underlayerPlan.toString());
	}
}