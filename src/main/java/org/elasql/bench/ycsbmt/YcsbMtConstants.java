package org.elasql.bench.ycsbmt;

import org.vanilladb.bench.ycsb.YcsbConstants;

public class YcsbMtConstants {
	public static final int INIT_RECORDS_PER_PART = 10_000_000;

	public static final boolean IS_DYNAMIC_FIELD_COUNT = true;
	public static final int FIELD_COUNT_IF_FIXED = 10; // including primary key
	
	public static final int CHARS_PER_FIELD = 33; 		// each char 3 bytes
	public static final String ID_FORMAT = "%0" + YcsbConstants.CHARS_PER_FIELD + "d";
}
