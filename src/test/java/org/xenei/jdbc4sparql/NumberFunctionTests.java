package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NumberFunctionTests extends AbstractJ4SSetup
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

		fUrl = J4SDriverTest.class.getResource("./J4SNumberFunctionTest.ttl");

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
	public void testAbsFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select abs( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("FOB", rset.getString(1));
		rset.close();
	}

	@Test
	public void testCeilFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select ceil( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("FOB", rset.getString(1));
		rset.close();
	}

	@Test
	public void testCountFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select count( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3, rset.getInt(1));
		rset.close();

	}

	@Test
	public void testFloorFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select floor( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(3, rsm.getColumnCount());
		rset.next();	
		Assert.assertEquals("FOB", rset.getString(1));
		rset.close();
	}

	@Test
	public void testMaxFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select max( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(7, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testMinFunction() throws Exception
	{

		// count all the rows
		final ResultSet rset = stmt
				.executeQuery("select min( IntCol ) from fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testRoundFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select round( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("FOB", rset.getString(1));
		rset.close();
	}

	@Test
	public void testSumFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select sum( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(15, rset.getInt(1));
		rset.close();
	}
	
	@Test
	public void testRandFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select rand() From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		double d = rset.getDouble(1);
		Assert.assertTrue(d > 0.0);
		Assert.assertTrue(d < 1.0);
		rset.close();
	}
}
