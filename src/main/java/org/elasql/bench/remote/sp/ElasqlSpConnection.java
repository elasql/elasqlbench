package org.elasql.bench.remote.sp;

import java.sql.Connection;
import java.sql.SQLException;

import org.elasql.remote.groupcomm.client.GroupCommConnection;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;

public class ElasqlSpConnection implements SutConnection {
	private GroupCommConnection conn;
	private int connectionId;

	public ElasqlSpConnection(GroupCommConnection conn, int connId) {
		this.conn = conn;
		this.connectionId = connId;
	}

	@Override
	public SutResultSet callStoredProc(int pid, Object... pars)
			throws SQLException {
		SpResultSet r = conn.callStoredProc(connectionId, pid, pars);
		return new ElasqlSpResultSet(r);
	}

	@Override
	public Connection toJdbcConnection() {
		throw new RuntimeException("ElaSQL does not support JDBC.");
	}
}

