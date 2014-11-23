package org.xenei.jdbc4sparql;

import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NumberFunctionTests extends AbstractJ4SSetup {
	// file URL
	private URL fUrl;

	// J4SUrl
	private String url;

	@Before
	public void setup() throws Exception {
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
	public void testAbsIntFunction() throws Exception {
		List<Integer> lst = new ArrayList<Integer>();
		final ResultSet rset = stmt
				.executeQuery("select abs( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		while (rset.next()) {
			lst.add(Integer.valueOf(rset.getInt(1)));
		}
		Assert.assertTrue(lst.contains(Integer.valueOf(5)));
		Assert.assertTrue(lst.contains(Integer.valueOf(3)));
		Assert.assertTrue(lst.contains(Integer.valueOf(7)));
		rset.close();
	}

	@Test
	public void testAbsDoubleFunction() throws Exception {
		List<Double> lst = new ArrayList<Double>();
		final ResultSet rset = stmt
				.executeQuery("select abs( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		while (rset.next()) {
			lst.add(Double.valueOf(rset.getDouble(1)));
		}
		Assert.assertTrue(lst.contains(Double.valueOf(1.3)));
		;
		Assert.assertTrue(lst.contains(Double.valueOf(1.5)));
		;
		Assert.assertTrue(lst.contains(Double.valueOf(1.7)));
		;
		rset.close();
	}

	@Test
	public void testCeilFunction() throws Exception {
		List<Integer> lst = new ArrayList<Integer>();
		final ResultSet rset = stmt
				.executeQuery("select ceil( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		while (rset.next()) {
			lst.add(rset.getInt(1));
		}
		Assert.assertTrue(lst.contains(Integer.valueOf(-1)));
		Assert.assertTrue(lst.contains(Integer.valueOf(2)));
		rset.close();
	}

	@Test
	public void testCountFunction() throws Exception {
		final ResultSet rset = stmt
				.executeQuery("select count( * ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testCountIntFunction() throws Exception {
		final ResultSet rset = stmt
				.executeQuery("select count( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testCountDoubleFunction() throws Exception {
		final ResultSet rset = stmt
				.executeQuery("select count( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(3, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testFloorFunction() throws Exception {
		List<Integer> lst = new ArrayList<Integer>();
		final ResultSet rset = stmt
				.executeQuery("select floor( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		while (rset.next()) {
			lst.add(Integer.valueOf(rset.getInt(1)));
		}
		Assert.assertTrue(lst.contains(Integer.valueOf(1)));
		Assert.assertTrue(lst.contains(Integer.valueOf(-2)));
		rset.close();
	}

	@Test
	public void testMaxIntFunction() throws Exception {

		final ResultSet rset = stmt
				.executeQuery("select max( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(7, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testMaxDoubleFunction() throws Exception {

		final ResultSet rset = stmt
				.executeQuery("select max( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(Double.valueOf(1.7),
				Double.valueOf(rset.getDouble(1)));
		rset.close();
	}

	@Test
	public void testMinIntFunction() throws Exception {
		final ResultSet rset = stmt
				.executeQuery("select min( IntCol ) from fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(-3, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testMinDoubleFunction() throws Exception {
		final ResultSet rset = stmt
				.executeQuery("select min( DoubleCol ) from fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(Double.valueOf(-1.3),
				Double.valueOf(rset.getDouble(1)));
		rset.close();
	}

	@Test
	public void testRoundFunction() throws Exception {
		List<Integer> lst = new ArrayList<Integer>();
		final ResultSet rset = stmt
				.executeQuery("select round( DoubleCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		while (rset.next()) {
			lst.add(Integer.valueOf(rset.getInt(1)));
		}
		Assert.assertTrue(lst.contains(Integer.valueOf(2)));
		Assert.assertTrue(lst.contains(Integer.valueOf(-1)));
		rset.close();
	}

	@Test
	public void testSumFunction() throws Exception {

		final ResultSet rset = stmt
				.executeQuery("select sum( IntCol ) From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		Assert.assertEquals(9, rset.getInt(1));
		rset.close();
	}

	@Test
	public void testRandFunction() throws Exception {

		final ResultSet rset = stmt.executeQuery("select rand() From fooTable");
		final ResultSetMetaData rsm = rset.getMetaData();
		Assert.assertEquals(1, rsm.getColumnCount());
		rset.next();
		double d = rset.getDouble(1);
		Assert.assertTrue(d > 0.0);
		Assert.assertTrue(d < 1.0);
		rset.close();
	}
}
