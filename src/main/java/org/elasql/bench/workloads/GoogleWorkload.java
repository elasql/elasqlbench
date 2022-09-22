package org.elasql.bench.workloads;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.util.RandomValueGenerator;

/**
 * The google workload reads a workload trace file and selects a partition
 * probabilistically following the distribution described in the trace file.
 * 
 * @author Yu-Shan Lin
 */
public class GoogleWorkload {
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	private static final String WORKLOAD_FILE;
	private static final int WORKLOAD_LENGTH;
	
	static {
		WORKLOAD_FILE = ElasqlBenchProperties.getLoader()
				.getPropertyAsString(GoogleWorkload.class.getName() + ".WORKLOAD_FILE", "");
		WORKLOAD_LENGTH = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlYcsbConstants.class.getName() + ".WORKLOAD_LENGTH", 0);
	}
	
	public static double[][] loadGoogleWorkload() {
		// Check file existence
		File file = new File(WORKLOAD_FILE);
		if (!file.exists())
			throw new RuntimeException(String.format("Path '%s' does not exist", WORKLOAD_FILE));
		if (!file.isFile())
			throw new RuntimeException(String.format("Path '%s' is not a file", WORKLOAD_FILE));
		
		// Load the data
		double[][] workload = new double[WORKLOAD_LENGTH][NUM_PARTITIONS];
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			// Data Format: Each row is a workload of a node, each value is the
			for (int partId = 0; partId < NUM_PARTITIONS; partId++) {
				String line = reader.readLine();
				String[] loads = line.split(",");
				for (int time = 0; time < WORKLOAD_LENGTH; time++) {
					workload[time][partId] = Double.parseDouble(loads[time]);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		return workload;
	}
	
	private double[][] workload;
	private int windowSize;
	
	public GoogleWorkload(int windowSize) {
		this.workload = loadGoogleWorkload();
		this.windowSize = windowSize;
	}
	
	public int randomlySelectPartId(long currentTime) {
		RandomValueGenerator rvg = new RandomValueGenerator();
		int timeIdx = (int) (currentTime / windowSize);
		return rvg.randomChooseFromDistribution(workload[timeIdx]);
	}
	
	public int getLength() {
		return WORKLOAD_LENGTH;
	}
}
