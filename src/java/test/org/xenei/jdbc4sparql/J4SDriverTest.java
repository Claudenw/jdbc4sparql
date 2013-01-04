package org.xenei.jdbc4sparql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class J4SDriverTest
{

	public static void main( final String[] args )
			throws ClassNotFoundException, SQLException
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		final String url = "jdbc:j4s?catalog=test:http://example.com";

		final Connection conn = DriverManager.getConnection(url, "myschema",
				"mypassw");

		conn.setAutoCommit(false);
		final Statement stmt = conn.createStatement();
		final ResultSet rset = stmt.executeQuery("select * from foo");
		while (rset.next())
		{
			System.out.println(rset.getString(1));
		}
		stmt.close();

		System.out.println("Success!");

	}

}
