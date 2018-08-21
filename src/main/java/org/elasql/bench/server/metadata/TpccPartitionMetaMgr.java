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

import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.sql.Constant;

public class TpccPartitionMetaMgr extends PartitionMetaMgr {

	public boolean isFullyReplicated(RecordKey key) {
		return key.getTableName().equals("item");
	}
	
	public static int getWarehouseId(RecordKey key) {
		// For other tables, partitioned by wid
		Constant widCon;
		switch (key.getTableName()) {
		case "warehouse":
			widCon = key.getKeyVal("w_id");
			break;
		case "district":
			widCon = key.getKeyVal("d_w_id");
			break;
		case "stock":
			widCon = key.getKeyVal("s_w_id");
			break;
		case "customer":
			widCon = key.getKeyVal("c_w_id");
			break;
		case "history":
			widCon = key.getKeyVal("h_c_w_id");
			break;
		case "orders":
			widCon = key.getKeyVal("o_w_id");
			break;
		case "new_order":
			widCon = key.getKeyVal("no_w_id");
			break;
		case "order_line":
			widCon = key.getKeyVal("ol_w_id");
			break;
		default:
			throw new IllegalArgumentException("cannot find proper partition rule for key:" + key);
		}
		
		return (Integer) widCon.asJavaVal();
	}

	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for TPC-C testbed. Partitions each
		 * table on warehouse id.
		 */
		
		// If is item table, return self node id
		// (items are fully replicated over all partitions)
		if (key.getTableName().equals("item"))
			return Elasql.serverId();

//		return getWarehouseId(key) - 1;
		return 0;
	}
}
