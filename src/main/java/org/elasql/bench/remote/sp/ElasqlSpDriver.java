package org.elasql.bench.remote.sp;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasql.remote.groupcomm.client.GroupCommConnection;
import org.elasql.remote.groupcomm.client.GroupCommDriver;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;

public class ElasqlSpDriver implements SutDriver {
	
	private static final AtomicInteger NEXT_CONNECTION_ID = new AtomicInteger(0);
	
	private static GroupCommConnection conn = null;
	
	public ElasqlSpDriver(int nodeId) {
		if (conn == null) {
			GroupCommDriver driver = new GroupCommDriver(nodeId);
			conn = driver.init();
		}
	}

	public SutConnection connectToSut() throws SQLException {
		try {
			// Each connection need a unique id
			return new ElasqlSpConnection(conn, NEXT_CONNECTION_ID.getAndIncrement());
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
	}
}
