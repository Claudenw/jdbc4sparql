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

public class StringFunctionTests extends AbstractJ4SSetup
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
	public void testLcaseFunction() throws Exception
	{

		ResultSet rset = stmt
				.executeQuery("select lcase( 'BAR' ) From fooTable");
		ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("bar", rset.getString(1));
		rset.close();

		rset = stmt.executeQuery("select lower( 'BAR' ) From fooTable");
		rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("bar", rset.getString(1));
		rset.close();
	}

	@Test
	public void testLengthFunction() throws Exception
	{

		// count all the rows
		ResultSet rset = stmt
				.executeQuery("select Length('foo') from fooTable");
		ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3, rset.getInt(1));
		rset.close();

		rset = stmt.executeQuery("select len('foo') from fooTable");
		rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3, rset.getInt(1));
		rset.close();

	}

	@Test
	public void testReplaceFunction() throws Exception
	{

		ResultSet rset = stmt
				.executeQuery("select replace( 'fob', 'o', 'a' ) From fooTable");
		ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("fab", rset.getString(1));
		rset.close();

		rset = stmt
				.executeQuery("select replace( 'fob', 'o', 'aa' ) From fooTable");
		rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("faab", rset.getString(1));
		rset.close();

		rset = stmt
				.executeQuery("select replace( 'fob.c', '.', 'aa' ) From fooTable");
		rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("fobaac", rset.getString(1));
		rset.close();

		rset = stmt
				.executeQuery("select replace( 'fobc', 'c', '\\' ) From fooTable");
		rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("fob\\", rset.getString(1));
		rset.close();
	}

	@Test
	public void testSubstringFunction() throws Exception
	{

		final ResultSet rset = stmt
				.executeQuery("select substring( 'fob', 2, 1 ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("o", rset.getString(1));
		rset.close();
	}

	@Test
	public void testUcaseFunction() throws Exception
	{

		ResultSet rset = stmt
				.executeQuery("select ucase( 'fob' ) From fooTable");
		ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("FOB", rset.getString(1));
		rset.close();

		rset = stmt.executeQuery("select upper( 'fob' ) From fooTable");
		rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals("FOB", rset.getString(1));
		rset.close();
	}
}
