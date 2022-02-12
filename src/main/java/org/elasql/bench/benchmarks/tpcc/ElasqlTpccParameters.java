package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.util.ElasqlBenchProperties;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.rte.TpccTxExecutor;
import org.vanilladb.bench.util.BenchProperties;

public class ElasqlTpccParameters {

	public static final boolean ENABLE_THINK_AND_KEYING_TIME;

	// Transaction frequency follows the mixture requirement
	public static final int FREQUENCY_TOTAL;
	public static final int FREQUENCY_NEW_ORDER;
	public static final int FREQUENCY_PAYMENT;
	public static final int FREQUENCY_ORDER_STATUS;
	public static final int FREQUENCY_DELIVERY;
	public static final int FREQUENCY_STOCK_LEVEL;
	
	public enum TpccPartitionStategy { NORMAL, MGCRAB_SCALING_OUT, MGCRAB_CONSOLIDATION };
	
	// 1: Normal, 2: MgCrab scaling-out, 3: MgCrab consolidation
	public static final TpccPartitionStategy PARTITION_STRATEGY;
	public static final int WAREHOUSE_PER_PART;
	
	static {
		ENABLE_THINK_AND_KEYING_TIME = BenchProperties.getLoader()
				.getPropertyAsBoolean(TpccTxExecutor.class.getName() +
						".ENABLE_THINK_AND_KEYING_TIME", false);
		
		FREQUENCY_TOTAL = BenchProperties.getLoader().getPropertyAsInteger(
				TpccConstants.class.getName() + ".FREQUENCY_TOTAL", 100);
		FREQUENCY_NEW_ORDER = BenchProperties.getLoader().getPropertyAsInteger(
				TpccConstants.class.getName() + ".FREQUENCY_NEW_ORDER", 45);
		FREQUENCY_PAYMENT = BenchProperties.getLoader().getPropertyAsInteger(
				TpccConstants.class.getName() + ".FREQUENCY_PAYMENT", 43);
		FREQUENCY_ORDER_STATUS = BenchProperties.getLoader().getPropertyAsInteger(
				TpccConstants.class.getName() + ".FREQUENCY_ORDER_STATUS", 4);
		FREQUENCY_DELIVERY = BenchProperties.getLoader().getPropertyAsInteger(
				TpccConstants.class.getName() + ".FREQUENCY_DELIVERY", 4);
		FREQUENCY_STOCK_LEVEL = BenchProperties.getLoader().getPropertyAsInteger(
				TpccConstants.class.getName() + ".FREQUENCY_STOCK_LEVEL", 4);
		
		int strategy = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlTpccParameters.class.getName() + ".PARTITION_STRATEGY", 1);
		switch (strategy) {
		case 2:
			PARTITION_STRATEGY = TpccPartitionStategy.MGCRAB_SCALING_OUT;
			break;
		case 3:
			PARTITION_STRATEGY = TpccPartitionStategy.MGCRAB_CONSOLIDATION;
			break;
		default:
			PARTITION_STRATEGY = TpccPartitionStategy.NORMAL;
			break;
		}
		WAREHOUSE_PER_PART = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlTpccParameters.class.getName() + ".WAREHOUSE_PER_PART", 1);
	}

	// Range for uniformly selecting transaction type
	public static final int RANGE_NEW_ORDER = FREQUENCY_NEW_ORDER;
	public static final int RANGE_PAYMENT = RANGE_NEW_ORDER + FREQUENCY_PAYMENT;
	public static final int RANGE_ORDER_STATUS = RANGE_PAYMENT
			+ FREQUENCY_ORDER_STATUS;
	public static final int RANGE_DELIVERY = RANGE_ORDER_STATUS
			+ FREQUENCY_DELIVERY;
	public static final int RANGE_STOCK_LEVEL = RANGE_DELIVERY
			+ FREQUENCY_STOCK_LEVEL;
}
