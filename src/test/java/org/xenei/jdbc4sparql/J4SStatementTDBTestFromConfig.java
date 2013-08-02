package org.xenei.jdbc4sparql;

import java.net.URL;
import java.util.Properties;

import org.junit.Before;

public class J4SStatementTDBTestFromConfig extends AbstractJ4SStatementTest
{

	@Before
	public void setup() throws Exception
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		final URL fUrl = J4SDriverTest.class
				.getResource("./J4SStatementTestTDB.zip");

		final J4SDriver driver = new J4SDriver();
		final J4SUrl url = new J4SUrl("jdbc:J4S:" + fUrl.toExternalForm());
		conn = new J4SConnection(driver, url, new Properties());
		stmt = conn.createStatement();
	}

}
