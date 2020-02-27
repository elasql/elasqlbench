package org.elasql.bench.benchmarks.ycsb.rte;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.BeforeClass;
import org.junit.Test;

public class MultitenantNormalParamGenTest {
		
	private static final int TENANT_RECORD_COUNT = 250000;
	private static final int PART_RECORD_COUNT = TENANT_RECORD_COUNT * 4;
	private static final int TX_RECORD_COUNT = 5;
	private static final int INSERT_COUNT = 3;
	
	@BeforeClass
	public static void setupProperties() {
		System.setProperty("org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART", "" + PART_RECORD_COUNT);
		System.setProperty("org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.TENANTS_PER_PART", "4");
		System.setProperty("org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE", "1.0");
		System.setProperty("org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.TX_RECORD_COUNT", "" + TX_RECORD_COUNT);
		System.setProperty("org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.ADD_INSERT_IN_WRITE_TX", "" + INSERT_COUNT);
		System.setProperty("org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.ZIPFIAN_PARAMETER", "0.99");
		System.setProperty("org.elasql.storage.metadata.PartitionMetaMgr.NUM_PARTITIONS", "4");
	}
	
	@Test
	public void checkFormat() {
		int tenantId = 0;
		MultitenantNormalParamGen gen = new MultitenantNormalParamGen(tenantId);
		Object[] params = gen.generateParameter();
		int paramCount = 0;
		
		// DB Type
		assertEquals(2, params[paramCount++]);
		
		// Read Count
		assertEquals(TX_RECORD_COUNT, params[paramCount++]);
		
		// Read Ids
		for (int i = 0; i < TX_RECORD_COUNT; i++) {
			assertEquals(tenantId, params[paramCount]);
			paramCount += 2;
		}
		
		// Write Count
		assertEquals(TX_RECORD_COUNT, params[paramCount++]);
		
		// Write Ids
		for (int i = 0; i < TX_RECORD_COUNT; i++) {
			assertEquals(tenantId, params[paramCount]);
			paramCount += 2;
		}
		
		// Write values
		for (int i = 0; i < TX_RECORD_COUNT; i++) {
			assertEquals(String.class, params[paramCount].getClass());
			paramCount++;
		}
		
		// Insert Count
		assertEquals(INSERT_COUNT, params[paramCount++]);
		
		// Insert Ids
		for (int i = 0; i < INSERT_COUNT; i++) {
			assertEquals(tenantId, params[paramCount]);
			paramCount += 2;
		}
		
		// Insert values
		for (int i = 0; i < INSERT_COUNT; i++) {
			assertEquals(String.class, params[paramCount].getClass());
			paramCount++;
		}
	}
	
	@Test
	public void testReadModifyWrite() {
		MultitenantNormalParamGen gen = new MultitenantNormalParamGen(1);
		HashSet<Integer> ids = new HashSet<Integer>();
		int startIndex;
		for (int i = 0; i < 100; i++) {
			Object[] params = gen.generateParameter();
			
			// Find all read ids
			startIndex = 3;
			for (int r = 0; r < TX_RECORD_COUNT; r++) {
				ids.add((Integer) params[startIndex + r * 2]);
			}
			
			// Find all read ids
			startIndex = 2 + 2 * TX_RECORD_COUNT + 2;
			for (int r = 0; r < TX_RECORD_COUNT; r++) {
				Integer id = (Integer) params[startIndex + r * 2];
				if (!ids.remove(id))
					fail("cannot find id " + id + ". Params: " + Arrays.toString(params));
			}
		}
	}
}
