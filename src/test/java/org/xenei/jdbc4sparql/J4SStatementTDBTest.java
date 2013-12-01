package org.xenei.jdbc4sparql;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.xenei.jdbc4sparql.config.MemDatasetProducer;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.DatasetProducer;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;

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
	}
	
	
}
