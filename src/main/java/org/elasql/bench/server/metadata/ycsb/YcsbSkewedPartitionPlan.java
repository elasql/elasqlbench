package org.elasql.bench.server.metadata.ycsb;

import org.elasql.bench.ycsb.ElasqlYcsbConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

// Assume there are 4 nodes and 16 tanents.
// We put 7 tanents on the first node and the rest 9 on the other 3 nodes.
public class YcsbSkewedPartitionPlan extends PartitionPlan {
	
	private static final long serialVersionUID = 1L;
	
	private static final int NUM_RECORDS = ElasqlYcsbConstants.RECORD_PER_PART * 4;
	private static final int RECORDS_ON_FIRST = NUM_RECORDS / 16 * 7;
	private static final int RECORDS_ON_OTHER = NUM_RECORDS / 16 * 3;
	
	public YcsbSkewedPartitionPlan() {
		// Check if there are 4 nodes
		if (PartitionMetaMgr.NUM_PARTITIONS != 4)
			throw new IllegalArgumentException("YcsbSkewedPartitionPlan can only operate on excat 4 nodes.");
	}
	
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
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
