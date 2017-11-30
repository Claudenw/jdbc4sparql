package org.xenei.jdbc4sparql;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;

public class J4SConnectionTest {
	private J4SDriver driver;
	private J4SUrl url;
	private J4SConnection connection;
	private Properties properties;

	@Before
	public void setUp() throws Exception {
		driver = new J4SDriver();
		final URL fUrl = J4SConnectionTest.class
				.getResource("./J4SDriverTest.ttl");
		url = new J4SUrl("jdbc:j4s?catalog=local&type=turtle:"
				+ fUrl.toExternalForm());
		properties = new Properties();
		properties.setProperty(J4SPropertyNames.DATASET_PRODUCER,
				"org.xenei.jdbc4sparql.config.MemDatasetProducer");
	}

	@After
	public void tearDown() throws Exception {
		connection.close();
	}

	@Test
	public void testAbort() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertFalse(connection.isClosed());
		connection.abort(null);
		Assert.assertTrue(connection.isClosed());
	}

	@Test
	public void testAddCatalog() throws Exception {
		final RdfCatalog.Builder builder = new RdfCatalog.Builder();
		builder.setName("testCatalog");
		connection = new J4SConnection(driver, url, properties);
		Assert.assertEquals(3, connection.getCatalogs().keySet().size());

		final RdfCatalog cat = connection.addCatalog(builder);
		Assert.assertNotNull(cat);
		Assert.assertFalse(connection.getCatalogs().isEmpty());
		Assert.assertEquals(4, connection.getCatalogs().keySet().size());
	}

	@Test
	public void testClearWarnings() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		connection.clearWarnings();
		Assert.assertNull(connection.getWarnings());
	}

	@Test
	public void testClose() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertFalse(connection.isClosed());
		connection.close();
		Assert.assertTrue(connection.isClosed());
	}

	@Test
	public void testCommit() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		connection.setAutoCommit(false);
		connection.commit();
		connection.setAutoCommit(true);
		try {
			connection.commit();
			Assert.fail("Should have thrown SQLException");
		} catch (final SQLException expected) {
		}
		connection.setAutoCommit(false);
		connection.commit();
		connection.close();
		try {
			connection.commit();
			Assert.fail("Should have thrown SQLException");
		} catch (final SQLException expected) {
		}
	}

	@Test
	public void testCreateArrayOf() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.createArrayOf(null, null);
			Assert.fail("should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {

		}
	}

	@Test
	public void testCreateBlob() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.createBlob();
			Assert.fail("should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {

		}
	}

	@Test
	public void testCreateClob() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.createClob();
			Assert.fail("should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {

		}
	}

	@Test
	public void testCreateNClob() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.createNClob();
			Assert.fail("should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {

		}
	}

	@Test
	public void testCreateSQLXML() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.createSQLXML();
			Assert.fail("should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {

		}
	}

	@Test
	public void testCreateStatement() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		final Statement stmt = connection.createStatement();
		Assert.assertNotNull(stmt);
	}

	@Test
	public void testCreateStatementIntInt() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		final Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		Assert.assertNotNull(stmt);
	}

	@Test
	public void testCreateStatementIntIntInt() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		final Statement stmt = connection.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
				ResultSet.HOLD_CURSORS_OVER_COMMIT);
		Assert.assertNotNull(stmt);
	}

	@Test
	public void testCreateStruct() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.createStruct(null, null);
			Assert.fail("should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {

		}
	}

	@Test
	public void testGetAutoCommit() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertTrue(connection.getAutoCommit());
		connection.setAutoCommit(false);
		Assert.assertFalse(connection.getAutoCommit());
	}

	@Test
	public void testGetCatalog() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		final String catName = connection.getCatalog();
		Assert.assertEquals("local", catName);
	}

	@Test
	public void testGetCatalogs() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		final Map<String, Catalog> map = connection.getCatalogs();
		Assert.assertEquals(3, map.size());
	}

	@Test
	public void testGetClientInfo() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertNotNull(connection.getClientInfo());
	}

	@Test
	public void testGetClientInfoString() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertNull(connection.getClientInfo("foo"));
	}

	@Test
	public void testGetHoldability() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT,
				connection.getHoldability());
	}

	@Test
	public void testGetMetaData() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertNotNull(connection.getMetaData());
	}

	@Test
	public void testGetNetworkTimeout() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertEquals(0, connection.getNetworkTimeout());
	}

	@Test
	public void testGetSchema() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertNull(connection.getSchema());
	}

	@Test
	public void testGetSparqlParser() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertNotNull(connection.getSparqlParser());
	}

	@Test
	public void testGetTransactionIsolation() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertEquals(Connection.TRANSACTION_NONE,
				connection.getTransactionIsolation());
	}

	@Test
	public void testGetTypeMap() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.getTypeMap();
			Assert.fail("should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {

		}
	}

	@Test
	public void testGetWarnings() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertNull(connection.getWarnings());
	}

	@Test
	public void testIsClosed() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertFalse(connection.isClosed());
		connection.close();
		Assert.assertTrue(connection.isClosed());
	}

	@Test
	public void testIsReadOnly() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertTrue(connection.isReadOnly());
	}

	@Test
	public void testIsValid() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertTrue(connection.isValid(3));

		try {
			Assert.assertTrue(connection.isValid(-1));
			Assert.fail("Should have thrown SQLException");
		} catch (final SQLException expected) {
		}
	}

	@Test
	public void testIsWrapperFor() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertFalse(connection.isWrapperFor(null));
		Assert.assertFalse(connection.isWrapperFor(Integer.class));
	}

	@Test
	public void testLoadConfig() throws Exception {
		final URL fUrl = J4SConnectionTest.class
				.getResource("./J4SStatementTest.zip");
		url = new J4SUrl("jdbc:j4s:" + fUrl.toExternalForm());
		connection = new J4SConnection(driver, url, properties);
		final Map<String, Catalog> map = connection.getCatalogs();
		Assert.assertEquals(3, map.keySet().size());
		Assert.assertTrue(map.containsKey("test"));
		Assert.assertTrue(map.containsKey(""));
		Assert.assertTrue(map.containsKey(MetaCatalogBuilder.LOCAL_NAME));
	}

	@Test
	public void testNativeSQL() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		final String s = connection.nativeSQL("select foo from aTable");
		Assert.assertEquals("SELECT foo FROM aTable", s);
	}

	@Test
	public void testPrepareCallString() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareCall("select foo from aTable");
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareCallStringIntInt() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareCall("select foo from aTable", 1, 2);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareCallStringIntIntInt() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareCall("select foo from aTable", 1, 2, 3);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareStatementString() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareStatement("select foo from aTable");
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareStatementStringInt() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareStatement("select foo from aTable", 1);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareStatementStringIntArray() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareStatement("select foo from aTable", new int[3]);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareStatementStringIntInt() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareStatement("select foo from aTable", 1, 2);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareStatementStringIntIntInt() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.prepareStatement("select foo from aTable", 1, 2, 3);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testPrepareStatementStringStringArray() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection
			.prepareStatement("select foo from aTable", new String[3]);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testReleaseSavepoint() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.releaseSavepoint(null);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testRollback() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.rollback();
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testRollbackSavepoint() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.rollback(null);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
		}
	}

	@Test
	public void testSaveConfig() throws Exception {
		connection = new J4SConnection(driver, url, properties);

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		connection.saveConfig(baos);
		baos.close();
		Assert.assertTrue(baos.size() > 0);
	}

	@Test
	public void testSetAutoCommit() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		connection.setAutoCommit(false);
		Assert.assertFalse(connection.getAutoCommit());
		connection.setAutoCommit(true);
		Assert.assertTrue(connection.getAutoCommit());
	}

	@Test
	public void testSetCatalog() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		connection.setCatalog(MetaCatalogBuilder.LOCAL_NAME);
		Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
				connection.getCatalog());
		try {
			connection.setCatalog("foo");
			Assert.fail("should have thrown SQLException");
		} catch (final SQLException expected) {
		}
		connection.setCatalog(MetaCatalogBuilder.LOCAL_NAME);
		Assert.assertEquals(MetaCatalogBuilder.LOCAL_NAME,
				connection.getCatalog());
	}

	@Test
	public void testSetClientInfoProperties() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		final Properties p = new Properties();
		p.setProperty("foo", "bar");
		connection.setClientInfo(p);
		Assert.assertEquals("bar", connection.getClientInfo("foo"));
		Assert.assertNull(connection.getClientInfo("baz"));
	}

	@Test
	public void testSetClientInfoStringString() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		Assert.assertNull(connection.getClientInfo("foo"));
		connection.setClientInfo("foo", "bar");
		String s = connection.getClientInfo("foo");
		Assert.assertEquals("bar", s);

		connection.setClientInfo("foo", "");
		s = connection.getClientInfo("foo");
		Assert.assertEquals("", s);

		connection.setClientInfo("foo", null);
		s = connection.getClientInfo("foo");
		Assert.assertNull(s);
	}

	@Test
	public void testSetHoldability() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

		try {
			connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException expected) {
			// exected
		}
	}

	@Test
	public void testSetNetworkTimeout() throws Exception {
		final Executor executor = Mockito.mock(Executor.class);
		connection = new J4SConnection(driver, url, properties);
		connection.setNetworkTimeout(executor, 5);
		Assert.assertEquals(5, connection.getNetworkTimeout());
	}

	@Test
	public void testSetReadOnly() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.setReadOnly(false);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException e) {
			// expected
		}
	}

	@Test
	public void testSetSavepoint() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.setSavepoint();
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException e) {
			// expected
		}
	}

	@Test
	public void testSetSavepointString() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.setSavepoint("foo");
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException e) {
			// expected
		}
	}

	@Test
	public void testSetSchema() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.setSchema("foo");
			Assert.fail("Should have thrown an excetion");
		} catch (final SQLException e) {
			Assert.assertEquals(
					"Schema 'foo' was not found in catalog 'local'",
					e.getMessage());
		}
		connection.setCatalog(MetaCatalogBuilder.LOCAL_NAME);
		
		connection.setSchema(MetaCatalogBuilder.SCHEMA_NAME);
		Assert.assertEquals(MetaCatalogBuilder.SCHEMA_NAME,
				connection.getSchema());
		connection.setSchema(null);
		Assert.assertNull(connection.getSchema());
	}

	@Test
	public void testSetTransactionIsolation() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.setTransactionIsolation(0);
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException e) {
			// expected
		}
	}

	@Test
	public void testSetTypeMap() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.setTypeMap(new HashMap<String, Class<?>>());
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException e) {
			// expected
		}
	}

	@Test
	public void testUnwrap() throws Exception {
		connection = new J4SConnection(driver, url, properties);
		try {
			connection.unwrap(this.getClass());
			Assert.fail("Should have thrown SQLFeatureNotSupportedException");
		} catch (final SQLFeatureNotSupportedException e) {
			// expected
		}
	}

}
