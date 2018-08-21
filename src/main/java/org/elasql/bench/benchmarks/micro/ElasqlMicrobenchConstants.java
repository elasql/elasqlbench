package org.elasql.bench.micro;

import org.elasql.bench.util.ElasqlBenchProperties;

public class ElasqlMicrobenchConstants {

	public static final int NUM_ITEMS_PER_NODE;
	
	static {
		NUM_ITEMS_PER_NODE = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlMicrobenchConstants.class.getName() + ".NUM_ITEMS_PER_NODE", 100000);
	}

}
