package org.xenei.jdbc4sparql;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;

import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class J4SStatementMemTest extends AbstractJ4SStatementTest
{
	// file URL
	private URL fUrl;

	// J4SUrl
	private String url;

	@Before
	public void setup() throws Exception
	{
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("com.hp.hpl.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		fUrl = J4SDriverTest.class.getResource("./J4SStatementTest.ttl");

		url = "jdbc:j4s?catalog=test&type=turtle&builder=org.xenei.jdbc4sparql.sparql.builders.SimpleNullableBuilder:"
				+ fUrl.toString();

		final Properties prop = new Properties();
		prop.setProperty(J4SPropertyNames.USER_PROPERTY, "myschema");
		prop.setProperty(J4SPropertyNames.PASSWORD_PROPERTY, "mypassw");
		conn = DriverManager.getConnection(url, prop);
		conn.setAutoCommit(false);
		stmt = conn.createStatement();
		// TODO remove this
		// ((J4SConnection)conn).saveConfig( new
		// java.io.File("/tmp/J4SStatementTest.zip"));
	}

	@Test
	public void testCountFromTable() throws Exception
	{

		// count all the rows
		final ResultSet rset = stmt
				.executeQuery("select count(*) from fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3L, rset.getLong(1));
		rset.close();

	}

	@Test
	public void testCountFromTableWithEqn() throws Exception
	{

		// count one row
		final ResultSet rset = stmt
				.executeQuery("select count(*) from fooTable where StringCol='Foo2String'");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(1L, rset.getLong(1));
		rset.close();

	}

	@Test
	public void testCountFunction() throws Exception
	{

		final String queryString = "SELECT (count(*) as ?x) where { ?fooTable <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/jdbc4sparql#fooTable> }";
		final Query query = QueryFactory.create(queryString);

		final List<QuerySolution> lqs = ((J4SConnection) conn).getCatalogs()
				.get(conn.getCatalog()).executeLocalQuery(query);
		Assert.assertEquals(1, lqs.size());
		final QuerySolution qs = lqs.get(0);
		Assert.assertEquals(3, qs.get("x").asLiteral().getValue());
	}

	@Test
	public void testCountWithAliasFromTable() throws Exception
	{
		// count all the rows
		final ResultSet rset = stmt
				.executeQuery("select count(*) as junk from fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3L, rset.getLong(1));
		Assert.assertEquals(3L, rset.getLong("junk"));
		rset.close();

		stmt.close();
	}
}
