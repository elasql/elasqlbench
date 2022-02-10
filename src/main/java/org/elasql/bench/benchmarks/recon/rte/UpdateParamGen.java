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
package org.elasql.bench.benchmarks.recon.rte;

import java.util.ArrayList;

import org.vanilladb.bench.benchmarks.recon.ReconbenchTransactionType;
import org.vanilladb.bench.benchmarks.tpcc.TpccValueGenerator;

public class UpdateParamGen extends ElasqlReconbenchParamGen {
	
	@Override
	public ReconbenchTransactionType getTxnType() {
		return ReconbenchTransactionType.UPDATE;
	}
	
	@Override
	public Object[] generateParameter() {
		TpccValueGenerator rvg = new TpccValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(getIndexUpdateCount());
		
		int localHotCount = LOCAL_HOT_COUNT;
		int mainPartition = 0;
		mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
		
		// Choose 2 local hot records for swap
		chooseHotData(paramList, mainPartition, localHotCount);
		chooseHotData(paramList, mainPartition, localHotCount);
		
		return paramList.toArray(new Object[0]);
	}

}