package org.elasql.bench.tpcc;

import org.elasql.storage.metadata.PartitionMetaMgr;

public class ElasqlTpccConstants {
	
	public static final int WAREHOUSE_PER_NODE = 20;
	public static final int ELASQL_NUM_WAREHOUSES =
			WAREHOUSE_PER_NODE * PartitionMetaMgr.NUM_PARTITIONS;
	
}
