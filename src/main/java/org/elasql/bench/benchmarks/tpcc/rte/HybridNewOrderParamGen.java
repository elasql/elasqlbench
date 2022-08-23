/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.bench.benchmarks.tpcc.rte;

import java.util.Random;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccBenchmark;
import org.elasql.bench.benchmarks.tpcc.ElasqlTpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.benchmarks.tpcc.TpccValueGenerator;
import org.vanilladb.bench.benchmarks.tpcc.rte.TpccTxParamGenerator;

public class HybridNewOrderParamGen implements TpccTxParamGenerator {
	
	private static final int WID_CHANGE_PERIOD_MS = 25;
	private static final int TYPE = ElasqlTpccRte.TYPE; 
	// 0: standard, 1: time dependent, 2: hybrid (2 hotspot in a window), 3: hybrid, 4: dynamic
	private static final double ORIGINAL_RTE_PERCENTAGE = 0.5; // for type 2
	private static final double SKEW_RATIO = 0.8;
	
	private int homeWid, homeDid;
	private TpccValueGenerator valueGen = new TpccValueGenerator();
	private int numOfWarehouses = ElasqlTpccBenchmark.getNumOfWarehouses();

	private Random random = new Random(0);
	
	public HybridNewOrderParamGen(int homeWarehouseId, int homeDistrictId) {
		homeWid = homeWarehouseId;
		homeDid = homeDistrictId;
	}

	@Override
	public TpccTransactionType getTxnType() {
		return TpccTransactionType.NEW_ORDER;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_NEW_ORDER * 1000;
	}

	@Override
	public Object[] generateParameter() {
		/*
		 * The return value of RandomChooseFromDistribution Method start from 1.
		 */
		// if (RemoteTerminalEmulator.IS_BATCH_REQUEST)
		// wid = rg.randomChooseFromDistribution(WAREHOUSE_DISTRIBUTION);

		boolean allLocal = true;
		// pars = {wid, did, cid, olCount, items[15][3], allLocal}
		int homeWid = getHomeWid();

		Object[] pars = new Object[50];
		pars[0] = homeWid;
		pars[1] = homeDid;
		pars[2] = valueGen.NURand(TpccValueGenerator.NU_CID, 1, TpccConstants.CUSTOMERS_PER_DISTRICT);
		// Note: We change olCount to 10, instead of a random number in 5~15
		// so that when we generate migration keys we will not have to scan the db
		// to ensure how many order line there are for each order.
//		int olCount = valueGen.number(5, 15);
		int olCount = 10;
		pars[3] = olCount;

		for (int i = 0; i < olCount; i++) {
			int j = 4 + i * 3;
			/*
			 * ol_i_id. 1% of the New-Order txs are chosen at random to simulate
			 * user data entry errors
			 */
			// if (rg.rng().nextDouble() < 0.01)
			// pars[j] = TpccConstants.NUM_ITEMS + 15; // choose unused item id
			// else
			pars[j] = valueGen.NURand(TpccValueGenerator.NU_OLIID, 1, TpccConstants.NUM_ITEMS);

			// TODO: Verify this
			// ol_supply_w_id. 1% of items are supplied by remote warehouse
			if (valueGen.rng().nextDouble() < 0.05 && numOfWarehouses > 1) {
				pars[++j] = valueGen.numberExcluding(1, numOfWarehouses, homeWid);
				allLocal = false;
			} else
				pars[++j] = homeWid;

			// ol_quantity
			pars[++j] = valueGen.number(1, 10);
		}
		pars[49] = allLocal;

		return pars;
	}

	@Override
	public long getThinkTime() {
		double r = valueGen.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_NEW_ORDER * 1000;
	}
	
	private int previousTime = -1;
	private int previosWareHouse = -1;
	
	private int getHomeWid() {
		switch (TYPE) { 
		case 0: 
			return this.homeWid; 
		case 1: 
			return (int) (System.currentTimeMillis() / WID_CHANGE_PERIOD_MS % numOfWarehouses) + 1; 
		case 2: // skewness must > 0
			if (random.nextDouble() < ORIGINAL_RTE_PERCENTAGE) 
				return this.homeWid;
			else
				return (int) (System.currentTimeMillis() / WID_CHANGE_PERIOD_MS % numOfWarehouses) + 1; 
		case 3: // skewness must > 0
			int currentWareHouse = (int) (System.currentTimeMillis() / WID_CHANGE_PERIOD_MS % numOfWarehouses) + 1;
			if (currentWareHouse != previousTime) {
				previousTime = currentWareHouse;
				if (random.nextDouble() < SKEW_RATIO) {
					previosWareHouse = this.homeWid;
				} else {
					previosWareHouse = currentWareHouse;
				}
			}
			return previosWareHouse;
		case 4:
			int startWid = ParamGenHelper.getPartId() * ElasqlTpccConstants.WAREHOUSE_PER_PART + 1;
			int widOffset = random.nextInt(ElasqlTpccConstants.WAREHOUSE_PER_PART);
			return (startWid + widOffset);
		default: 
			throw new UnsupportedOperationException(); 
		}
	}
}
