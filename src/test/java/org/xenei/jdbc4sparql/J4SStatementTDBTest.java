package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.DriverManager;
import java.util.Properties;

import org.junit.Before;

public class J4SStatementTDBTest extends AbstractJ4SStatementTest
{
	// file URL
	private URL fUrl;

	// J4SUrl
	private String url;

	@Before
	public void setup() throws Exception
	{
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		fUrl = J4SDriverTest.class.getResource("./J4SStatementTest.ttl");

		url = "jdbc:j4s?catalog=test&type=turtle&builder=org.xenei.jdbc4sparql.sparql.builders.SimpleNullableBuilder:"
				+ fUrl.toString();

		final Properties prop = new Properties();
		prop.setProperty(J4SPropertyNames.USER_PROPERTY, "myschema");
		prop.setProperty(J4SPropertyNames.PASSWORD_PROPERTY, "mypassw");
		prop.setProperty(J4SPropertyNames.DATASET_PRODUCER,
				"org.xenei.jdbc4sparql.config.TDBDatasetProducer");
		conn = DriverManager.getConnection(url, prop);
		conn.setAutoCommit(false);
		stmt = conn.createStatement();
		// TODO remove this
		// ((J4SConnection)conn).saveConfig( new
		// File("/tmp/J4SStatementTestTDB.zip"));
	}

}