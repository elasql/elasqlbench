package org.elasql.bench.workloads;

public class MultiTrendGoogleWorkload {
	
	private MultiTrendWorkload multiTrendWorkload;
	
	public MultiTrendGoogleWorkload(int longTermWindowSize, int shortTermWindowSize) {
		double[][] googleWorkload = GoogleWorkload.loadGoogleWorkload();
		multiTrendWorkload = new MultiTrendWorkload(googleWorkload, longTermWindowSize, shortTermWindowSize);
	}
	
	public int getShortTermFocusedPart(long currentTime) {
		return multiTrendWorkload.getShortTermFocusedPart(currentTime);
	}
	
	public int randomlySelectPartId(long currentTime) {
		return multiTrendWorkload.randomlySelectPartId(currentTime);
	}
}
