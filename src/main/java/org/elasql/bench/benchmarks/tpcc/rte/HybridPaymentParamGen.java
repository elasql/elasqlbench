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
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.benchmarks.tpcc.TpccValueGenerator;
import org.vanilladb.bench.benchmarks.tpcc.rte.TpccTxParamGenerator;

public class HybridPaymentParamGen implements TpccTxParamGenerator {
	
	private static final int WID_CHANGE_PERIOD_MS = 25;
	private static final int TYPE = ElasqlTpccRte.TYPE; 
	// 0: standard, 1: time dependent, 2: hybrid (2 hotspot in a window), 3: hybrid
	private static final double ORIGINAL_RTE_PERCENTAGE = 0.5; // for type 2
	private static final double SKEW_RATIO = 0.5;
	
	private int homeWid;
	private TpccValueGenerator valueGen = new TpccValueGenerator();
	private int numOfWarehouses = ElasqlTpccBenchmark.getNumOfWarehouses();

	private Random random = new Random(0);

	public HybridPaymentParamGen(int homeWarehouseId) {
		homeWid = homeWarehouseId;
	}

	@Override
	public TpccTransactionType getTxnType() {
		return TpccTransactionType.PAYMENT;
	}

	@Override
	public long getKeyingTime() {
		return TpccConstants.KEYING_PAYMENT * 1000;
	}

	@Override
	public Object[] generateParameter() {
		int homeWid = getHomeWid();
		
		// pars = {wid, did, cwid, cdid, cid/clast, hAmount}
		Object[] pars = new Object[6];
		pars[0] = homeWid;
		pars[1] = valueGen.number(1, 10);
		/*
		 * Customer resident warehouse is the home warehouse 85% of the time and
		 * is a randomly selected remote warehouse 15% of the time.
		 */
		if (valueGen.rng().nextDouble() >= 0.85 && numOfWarehouses > 1) {
			pars[2] = valueGen.numberExcluding(1, numOfWarehouses, homeWid);
			pars[3] = valueGen.number(1, 10);
		} else {
			pars[2] = homeWid;
			pars[3] = pars[1];
		}

		/*
		 * The customer is randomly selected 60% of the time by last name and
		 * 40% of time by id.
		 */
		// XXX: ElaSQL doesn't support selecting by the last name
//		if (rg.rng().nextDouble() >= 0.60)
//			pars[4] = rg.makeRandomLastName(false);
//		else
			pars[4] = valueGen.NURand(TpccValueGenerator.NU_CID, 1,
					TpccConstants.CUSTOMERS_PER_DISTRICT);
		pars[5] = valueGen.fixedDecimalNumber(2, 1.00, 5000.00);
		return pars;
	}

	@Override
	public long getThinkTime() {
		double r = valueGen.rng().nextDouble();
		return (long) -Math.log(r) * TpccConstants.THINKTIME_PAYMENT * 1000;
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
		default: 
			throw new UnsupportedOperationException(); 
		}
	}
}
