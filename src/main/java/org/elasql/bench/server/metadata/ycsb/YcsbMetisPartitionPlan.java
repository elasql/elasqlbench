package org.elasql.bench.server.metadata.ycsb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

// XXX: Need to be checked before using
public class YcsbMetisPartitionPlan extends PartitionPlan {
	private static Logger logger = Logger.getLogger(YcsbMetisPartitionPlan.class.getName());
	
	private static final long serialVersionUID = 1L;
	
	private static final int METIS_DATA_RANGE = 1;
	private static final int VERTEX_PER_PART = ElasqlYcsbConstants.RECORD_PER_PART / METIS_DATA_RANGE;
	
	private PartitionPlan underlayerPlan;
	private Map<Integer, Integer> metisPlan = new HashMap<Integer, Integer>();
	
	public YcsbMetisPartitionPlan(PartitionPlan defaultPlan, String metisFilePath) {
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

			String line;
//			Map<String, Constant> keyEntryMap;
			int lineCount = 0;
			while ((line = br.readLine()) != null) {
				int newPartId = Integer.parseInt(line);
				metisPlan.put(lineCount, newPartId);
				
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
		return underlayerPlan.isFullyReplicated(key);
	}
	
	public int convertToVertexId(int ycsbId)
	{
		ycsbId -= 1; // [1, N] => [0, N-1]
		int partId = ycsbId / ElasqlYcsbConstants.MAX_RECORD_PER_PART;
		int vertexIdInPart = (ycsbId % ElasqlYcsbConstants.MAX_RECORD_PER_PART) / METIS_DATA_RANGE;
		return partId * VERTEX_PER_PART + vertexIdInPart;
	}

	public int getPartition(RecordKey key) {
		// Check the metis first
		Constant idCon = key.getKeyVal("ycsb_id");
		if (idCon != null) {
			int ycsbId = Integer.parseInt((String) idCon.asJavaVal());
			int vid = convertToVertexId(ycsbId);
			Integer id = metisPlan.get(vid);
			
			if (id != null)
				return id;
		}
			
		// If not found, check the underlayer plan
		return underlayerPlan.getPartition(key);
	}

	@Override
	public PartitionPlan getBasePartitionPlan() {
		return this;
	}

	@Override
	public boolean isBasePartitionPlan() {
		return true;
	}

	@Override
	public void changeBasePartitionPlan(PartitionPlan plan) {
		throw new RuntimeException("There is no base partition plan in "
				+ "YcsbMetisPartitionPlan that can be changed");
	}
}