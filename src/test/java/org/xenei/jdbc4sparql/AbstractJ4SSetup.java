package org.xenei.jdbc4sparql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJ4SSetup {

	// JDBC Connection
	protected Connection conn;

	protected Statement stmt;

	static private Logger LOG = LoggerFactory.getLogger(AbstractJ4SSetup.class);

	protected List<String> getColumnNames(final String table)
			throws SQLException {
		final ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(),
				conn.getSchema(), table, null);
		final List<String> colNames = new ArrayList<String>();
		while (rs.next()) {
			if (LOG.isDebugEnabled()) {
				AbstractJ4SSetup.LOG.debug(String.format("%s %s %s %s",
						rs.getString(1), rs.getString(2), rs.getString(3),
						rs.getString(4)));
			}
			colNames.add(rs.getString(4));
		}
		return colNames;
	}

	@After
	public void tearDown() {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (final SQLException ignore) {
		}
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (final SQLException ignore) {
		}
	}

}
