package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;

public class J4SStatementMemTestFromConfig extends AbstractJ4SStatementTest
{

	@Before
	public void setup() throws Exception
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		final URL fUrl = J4SDriverTest.class
				.getResource("./J4SStatementTest.zip");

		final J4SDriver driver = new J4SDriver();
		final J4SUrl url = new J4SUrl("jdbc:J4S:" + fUrl.toExternalForm());
		conn = new J4SConnection(driver, url, new Properties());
		stmt = conn.createStatement();
		
		///
		
		final ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(),
				conn.getSchema(), "fooTable", null);
		final List<String> colNames = new ArrayList<String>();
		while (rs.next())
		{
			// TODO remove this
			System.out.println(String.format("%s %s %s %s", rs.getString(1),
					rs.getString(2), rs.getString(3), rs.getString(4)));
			colNames.add(rs.getString(4));
		}
		System.out.println( colNames.size());
	}

}
