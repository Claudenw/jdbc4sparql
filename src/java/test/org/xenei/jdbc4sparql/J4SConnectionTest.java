package org.xenei.jdbc4sparql;

import static org.junit.Assert.*;

import com.hp.hpl.jena.rdf.model.ModelFactory;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;

public class J4SConnectionTest
{
	private J4SDriver driver;
	private J4SUrl url;
	private J4SConnection connection;
	private Properties properties;

	@Before
	public void setUp() throws Exception
	{
		driver = new J4SDriver();
		URL fUrl = J4SConnectionTest.class.getResource("./J4SDriverTest.ttl");
		url = new J4SUrl("jdbc:j4s?type=turtle:"+fUrl.toExternalForm());
		properties = new Properties();
	}

	@After
	public void tearDown() throws Exception
	{
		connection.close();
	}

	@Test
	public void testLoadConfig() throws Exception
	{
		URL fUrl = J4SConnectionTest.class.getResource("./config.zip");
		url = new J4SUrl("jdbc:j4s:"+fUrl.toExternalForm());
		connection = new J4SConnection(driver, url, properties );
		Map<String,Catalog> map = connection.getCatalogs();
		Assert.assertEquals( 2, map.keySet().size());
		Assert.assertTrue( map.containsKey( "" ));
		Assert.assertTrue( map.containsKey( MetaCatalogBuilder.LOCAL_NAME));
	}

	@Test
	public void testAbort() throws Exception
	{
		Assert.assertFalse( connection.isClosed() );
		connection.abort( null );
		Assert.assertTrue( connection.isClosed() );
	}

	@Test
	public void testAddCatalog() throws Exception
	{
		RdfCatalog.Builder builder = new RdfCatalog.Builder();
		builder.setName( "testCatalog");
		connection = new J4SConnection(driver, url, properties );
		assertEquals( 2, connection.getCatalogs().keySet().size() );
		
		RdfCatalog cat = connection.addCatalog(builder);
		assertNotNull( cat );
		assertFalse( connection.getCatalogs().isEmpty());
		assertEquals( 3, connection.getCatalogs().keySet().size() );
	}

	@Test
	public void testClearWarnings() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		connection.clearWarnings();
		assertNull( connection.getWarnings() );
	}

	@Test
	public void testClose() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		assertFalse( connection.isClosed());
		connection.close();
		assertTrue( connection.isClosed());
	}

	@Test
	public void testCommit() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		connection.setAutoCommit( false );
		connection.commit();
		connection.setAutoCommit(true);
		try {
			connection.commit();
			fail( "Should have thrown SQLException");
		}
		catch (SQLException expected)
		{
		}
		connection.setAutoCommit(false);
		connection.commit();
		connection.close();
		try {
			connection.commit();
			fail( "Should have thrown SQLException");
		}
		catch (SQLException expected)
		{
		}
	}

	@Test
	public void testCreateArrayOf() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.createArrayOf( null, null );
			fail( "should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
			
		}
	}

	@Test
	public void testCreateBlob()throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.createBlob();
			fail( "should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
			
		}
	}

	@Test
	public void testCreateClob()throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.createClob(  );
			fail( "should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
			
		}
	}

	@Test
	public void testCreateNClob()throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.createNClob(  );
			fail( "should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
			
		}
	}

	@Test
	public void testCreateSQLXML() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.createSQLXML(  );
			fail( "should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
			
		}
	}
	

	@Test
	public void testCreateStatement() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Statement stmt = connection.createStatement();
		assertNotNull( stmt );
	}

	@Test
	public void testCreateStatementIntInt() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Statement stmt = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY );
		assertNotNull( stmt );
	}

	@Test
	public void testCreateStatementIntIntInt() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Statement stmt = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY,  ResultSet.HOLD_CURSORS_OVER_COMMIT);
		assertNotNull( stmt );
	}

	@Test
	public void testCreateStruct() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.createStruct( null, null );
			fail( "should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
			
		}
	}

	@Test
	public void testGetAutoCommit() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertTrue( connection.getAutoCommit() );
		connection.setAutoCommit( false );
		Assert.assertFalse( connection.getAutoCommit() );
	}

	@Test
	public void testGetCatalog() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		String catName = connection.getCatalog();
		Assert.assertEquals( "", catName );
	}

	@Test
	public void testGetCatalogs() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Map<String, Catalog> map = connection.getCatalogs();
		Assert.assertEquals( 2, map.size() );	
	}

	@Test
	public void testGetClientInfo() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertNull( connection.getClientInfo() );
	}

	@Test
	public void testGetClientInfoString() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertNull( connection.getClientInfo( "foo" ) );
	}

	@Test
	public void testGetHoldability() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertEquals( ResultSet.HOLD_CURSORS_OVER_COMMIT, connection.getHoldability() );
	}

	@Test
	public void testGetMetaData() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertNotNull( connection.getMetaData() );
	}

	@Test
	public void testGetNetworkTimeout() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertEquals( 0, connection.getNetworkTimeout() );
	}

	@Test
	public void testGetSchema() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertNotNull( connection.getSchema() );
	}

	@Test
	public void testGetSparqlParser() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertNotNull( connection.getSparqlParser() );
	}

	@Test
	public void testGetTransactionIsolation() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation() );
	}

	@Test
	public void testGetTypeMap() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.getTypeMap();
			fail( "should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
			
		}
	}

	@Test
	public void testGetWarnings() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertNull(connection.getWarnings() );
	}

	@Test
	public void testIsClosed() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertFalse(connection.isClosed() );
		connection.close();
		Assert.assertTrue(connection.isClosed() );
	}

	@Test
	public void testIsReadOnly() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertTrue(connection.isReadOnly() );
	}

	@Test
	public void testIsValid() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertTrue(connection.isValid( 3 ) );
		
		try {
			Assert.assertTrue(connection.isValid( -1 ) );	
			Assert.fail( "Should have thrown SQLException");
		}
		catch (SQLException expected)
		{
		}
	}

	@Test
	public void testIsWrapperFor() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		Assert.assertFalse(connection.isWrapperFor( null ) );
		Assert.assertFalse(connection.isWrapperFor(  Integer.class ) );
	}

	@Test
	public void testNativeSQL() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		String s = connection.nativeSQL( "select foo from aTable");
		Assert.assertEquals( "SELECT foo FROM aTable", s );
	}

	@Test
	public void testPrepareCallString() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareCall( "select foo from aTable");
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}

	@Test
	public void testPrepareCallStringIntInt() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareCall( "select foo from aTable", 1 ,2 );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}

	@Test
	public void testPrepareCallStringIntIntInt() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareCall( "select foo from aTable", 1 ,2, 3 );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}


	@Test
	public void testPrepareStatementString() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareStatement( "select foo from aTable" );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}


	@Test
	public void testPrepareStatementStringInt() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareStatement( "select foo from aTable", 1 );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}

	@Test
	public void testPrepareStatementStringIntInt() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareStatement( "select foo from aTable", 1, 2 );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}

	@Test
	public void testPrepareStatementStringIntIntInt() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareStatement( "select foo from aTable", 1, 2 ,3 );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}
	@Test
	public void testPrepareStatementStringIntArray() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareStatement( "select foo from aTable", new int[3] );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}

	@Test
	public void testPrepareStatementStringStringArray() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.prepareStatement( "select foo from aTable", new String[3] );
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}

	@Test
	public void testReleaseSavepoint() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.releaseSavepoint(null);
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}

	@Test
	public void testRollback() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.rollback();
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}


	@Test
	public void testRollbackSavepoint() throws Exception 
	{
		connection = new J4SConnection(driver, url, properties );
		try {
			connection.rollback(null);
			Assert.fail( "Should have thrown SQLFeatureNotSupportedException");
		}
		catch (SQLFeatureNotSupportedException expected)
		{
		}
	}


	@Test
	public void testSaveConfig() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		connection.saveConfig( baos );
		baos.close();
		Assert.assertTrue( baos.size() > 0);
	}

	@Test
	public void testSetAutoCommit() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		connection.setAutoCommit( false );
		Assert.assertFalse( connection.getAutoCommit() );
		connection.setAutoCommit( true );
		Assert.assertTrue( connection.getAutoCommit() );	
	}

	@Test
	public void testSetCatalog() throws Exception
	{
		connection = new J4SConnection(driver, url, properties );
		connection.setCatalog( "" );
		Assert.assertEquals( "", connection.getCatalog());
		try {
			connection.setCatalog( "foo" );
			fail( "should have thrown SQLException");
		}
		catch (SQLException expected) {
		}
		connection.setCatalog( MetaCatalogBuilder.LOCAL_NAME );
		Assert.assertEquals( MetaCatalogBuilder.LOCAL_NAME, connection.getCatalog());
	}

	@Test
	public void testSetClientInfoProperties()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetClientInfoStringString()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetHoldability()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetNetworkTimeout()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetReadOnly()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetSavepoint()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetSavepointString()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetSchema()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetTransactionIsolation()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetTypeMap()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testUnwrap()
	{
		fail("Not yet implemented");
	}

}
