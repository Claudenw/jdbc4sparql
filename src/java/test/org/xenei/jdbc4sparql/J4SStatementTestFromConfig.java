package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class J4SStatementTestFromConfig extends AbstractJ4SStatementTest
{
	
	@Before
	public void setup() throws Exception
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		URL fUrl = J4SDriverTest.class.getResource("./J4SStatementTest.zip"); // /org/xenei/jdbc4sparql/J4SDriverTest.ttl");

		//final URL fUrl = J4SDriverTest.class.getResource("./rdf_am-config.ttl");

		final J4SDriver driver = new J4SDriver();
		final J4SUrl url = new J4SUrl("jdbc:J4S:" + fUrl.toExternalForm());
		conn = new J4SConnection(driver, url, new Properties());
		conn.setCatalog("test");
		stmt = conn.createStatement();
		//conn.setCatalog("catalog");
		//conn.setSchema("schema");
	}

	
}
