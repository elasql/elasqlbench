package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.migration.MigrationRangeUpdate;

public class TpccMigrationRangeUpdate implements MigrationRangeUpdate, Serializable {
	
	private static final long serialVersionUID = 20181101001L;
	
	int minWid;
	TpccKeyIterator unmigratedKeys;
	int sourcePartId, destPartId;
	
	TpccMigrationRangeUpdate(int sourcePartId, int destPartId,
			int minWid, TpccKeyIterator unmigratedKeys) {
		this.minWid = minWid;
		this.unmigratedKeys = unmigratedKeys;
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
	}
	
	@Override
	public int getSourcePartId() {
		return sourcePartId;
	}

	@Override
	public int getDestPartId() {
		return destPartId;
	}
}
