package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class J4SStatementTest2
{
	private J4SConnection connection;

	@Test
	public void checkResults() throws SQLException
	{

		final java.sql.Statement stmt = connection.createStatement();
		final ResultSet rs = stmt.executeQuery("Select * from Person");
		final ResultSetMetaData rmd = rs.getMetaData();

		for (int i = 1; i <= rmd.getColumnCount(); i++)
		{
			System.out.println(String.format("%s - %s", rmd.getColumnName(i),
					rmd.isNullable(i)));
		}
		/*
		 * while (rs.next())
		 * {
		 * System.out.println( rs.getString(1));
		 * }
		 * rs.close();
		 * stmt.close();
		 */

		// J4SUrl url = new J4SUrl("jdbc:J4S?type=ttl:"
		// + fUrl.toURI().normalize().toASCIIString());
		//
		// final J4SDriver driver = new J4SDriver();
		// final J4SConnection connection = new J4SConnection(driver, url,
		// null);
		// connection.saveConfig( new ModelWriter( System.out ));
		//
		// connection.saveConfig( new ModelWriter( cfgFile ));

	}

	@Test
	public void getMetaData() throws SQLException
	{
		final DatabaseMetaData dmd = connection.getMetaData();
		final ResultSet rs = dmd.getTables("catalog", "schema", null, null);
		while (rs.next())
		{
			listTable(dmd, rs.getString("TABLE_CAT"),
					rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
		}
	}

	private void listTable( final DatabaseMetaData dmd, final String catalog,
			final String schema, final String table ) throws SQLException
	{

		final ResultSet rs = dmd.getColumns(catalog, schema, table, null);
		while (rs.next())
		{
			System.out.println(String.format(
					"Cat %s Schema %s tbl %s col %s Nullable: %s", catalog,
					schema, table, rs.getString("COLUMN_NAME"),
					rs.getString("IS_NULLABLE")));
		}
	}

	@Before
	public void setup() throws Exception
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		final URL fUrl = J4SDriverTest.class.getResource("./rdf_am-config.ttl");

		final J4SDriver driver = new J4SDriver();
		final J4SUrl url = new J4SUrl("jdbc:J4S:" + fUrl.toExternalForm());
		connection = new J4SConnection(driver, url, null);
		connection.setCatalog("catalog");
		connection.setSchema("schema");
	}

	@After
	public void tearDown()
	{
		try
		{
			connection.close();
		}
		catch (final SQLException ignore)
		{
		}
	}
}
