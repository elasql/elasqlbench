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
package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.micro.ElasqlMicrobenchConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;

public class MicroBenchPartitionMetaMgr extends PartitionMetaMgr {

	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */
		Constant iidCon = key.getKeyVal("i_id");
		if (iidCon != null) {
			int iid = (int) iidCon.asJavaVal();
			return (iid - 1) / ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
	}
}
