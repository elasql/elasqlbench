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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.TransactionType;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.benchmarks.tpcc.rte.TpccTxExecutor;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;

public class ElasqlTpccRte extends RemoteTerminalEmulator<TpccTransactionType> {
	
	private int homeWid;
	private static Random txnTypeRandom;
	private Map<TransactionType, TpccTxExecutor> executors;

	public ElasqlTpccRte(SutConnection conn, StatisticMgr statMgr, int homeWarehouseId, int homeDistrictId) {
		super(conn, statMgr);
		homeWid = homeWarehouseId;
		txnTypeRandom = new Random();
		executors = new HashMap<TransactionType, TpccTxExecutor>();
		executors.put(TpccTransactionType.NEW_ORDER, new TpccTxExecutor(new NewOrderParamGen(homeWid, homeDistrictId)));
		executors.put(TpccTransactionType.PAYMENT, new TpccTxExecutor(new PaymentParamGen(homeWid)));
		// TODO: Not implemented
//		executors.put(TpccTransactionType.ORDER_STATUS, new TpccTxExecutor(new OrderStatusParamGen(homeWid)));
//		executors.put(TpccTransactionType.DELIVERY, new TpccTxExecutor(new DeliveryParamGen(homeWid)));
//		executors.put(TpccTransactionType.STOCK_LEVEL, new TpccTxExecutor(new StockLevelParamGen(homeWid)));
	}
	
	protected TpccTransactionType getNextTxType() {
		int index = txnTypeRandom.nextInt(TpccConstants.FREQUENCY_TOTAL);
		if (index < TpccConstants.RANGE_NEW_ORDER)
			return TpccTransactionType.NEW_ORDER;
		else if (index < TpccConstants.RANGE_PAYMENT)
			return TpccTransactionType.PAYMENT;
		else if (index < TpccConstants.RANGE_ORDER_STATUS)
			return TpccTransactionType.ORDER_STATUS;
		else if (index < TpccConstants.RANGE_DELIVERY)
			return TpccTransactionType.DELIVERY;
		else
			return TpccTransactionType.STOCK_LEVEL;
	}
	
	protected TransactionExecutor<TpccTransactionType> getTxExeutor(TpccTransactionType type) {
		return executors.get(type);
	}
}
