package org.elasql.bench.workloads;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.vanilladb.bench.util.RandomValueGenerator;

/**
 * A workload generator that generates short-term skewness based on a given long-term workload.
 * The short-term workload will always provide the same partition id at the same time in order
 * to create a short-term skewness on the target partition, but it will still follow the
 * long-term load distribution provided by the user by periodically changing the target partition.
 * This is thread-safe.
 * 
 * @author Yu-Shan Lin
 */
public class MultiTrendWorkload {
	
	private static int[] generateShortTermSequence(double[] longTermDistribution, int seqLength) {
		// Calculate the sum
		double sum = 0.0;
		for (int i = 0; i < longTermDistribution.length; i++) {
			sum += longTermDistribution[i];
		}

		// Normalize the distribution to counts
		int[] counts = new int[longTermDistribution.length];
		for (int i = 0; i < counts.length; i++) {
			counts[i] = (int) (longTermDistribution[i] / sum * seqLength);
		}
		
		// Generate the sequence
		List<Integer> seqeuence = new ArrayList<Integer>(seqLength);
		for (int partId = 0; partId < counts.length; partId++) {
			Integer boxedPartId = partId;
			for (int i = 0; i < counts[partId]; i++)
				seqeuence.add(boxedPartId);
		}
		
		// Fill up the rest space (if there is any) with the last partition id
		while (seqeuence.size() < seqLength)
			seqeuence.add(counts.length - 1);
		
		// Shuffle to create randomness (deterministic)
		Collections.shuffle(seqeuence, new Random(12345678));
		
		// Save as an integer array
		int[] intArray = seqeuence.stream().mapToInt(Integer::intValue).toArray();
		
		return intArray;
	}
	
	private int longTermWindowSize;
	private int shortTermWindowSize;
	private int shortTermWindowCount;
	
	private double[][] longTermWorkload;
	private int[][] shortTermFcousParts;
	
	public MultiTrendWorkload(double[][] longTermWorkload, int longTermWindowSize, int shortTermWindowSize) {
		this.longTermWorkload = longTermWorkload;
		this.longTermWindowSize = longTermWindowSize;
		this.shortTermWindowSize = shortTermWindowSize;
		
		if (longTermWindowSize % shortTermWindowSize != 0) {
			throw new IllegalArgumentException(String.format(
					"The long-term window size (%d) must be excatly divdied by the short-term window size (%d).",
					longTermWindowSize, shortTermWindowSize));
		}
		
		this.shortTermWindowCount = longTermWindowSize / shortTermWindowSize;
		this.shortTermFcousParts = new int[longTermWorkload.length][shortTermWindowCount];
		for (int timeIdx = 0; timeIdx < longTermWorkload.length; timeIdx++) {
			shortTermFcousParts[timeIdx] = generateShortTermSequence(longTermWorkload[timeIdx], shortTermWindowCount);
		}
	}
	
	public int getShortTermFocusedPart(long currentTime) {
		int longTermIdx = (int) (currentTime / longTermWindowSize);
		int timeSlotId = (int) (currentTime % longTermWindowSize / shortTermWindowSize);
		return shortTermFcousParts[longTermIdx][timeSlotId];
	}
	
	public int randomlySelectPartId(long currentTime) {
		RandomValueGenerator rvg = new RandomValueGenerator();
		int longTermIdx = (int) (currentTime / longTermWindowSize);
		return rvg.randomChooseFromDistribution(longTermWorkload[longTermIdx]);
	}
	
	public int getLongTermLength() {
		return longTermWorkload.length;
	}
}
