package org.xenei.jdbc4sparql;

import static org.junit.Assert.*;

import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xenei.jdbc4sparql.iface.Catalog;
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
	public void testJ4SConnection()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testAbort()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testAddCatalog()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testClearWarnings()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testClose()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCommit()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateArrayOf()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateBlob()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateClob()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateNClob()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateSQLXML()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateStatement()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateStatementIntInt()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateStatementIntIntInt()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testCreateStruct()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetAutoCommit()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetCatalog()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetCatalogs()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetClientInfo()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetClientInfoString()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetHoldability()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetMetaData()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetModelReader()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetNetworkTimeout()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetSchema()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetSparqlParser()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetTransactionIsolation()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetTypeMap()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testGetWarnings()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testIsClosed()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testIsReadOnly()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testIsValid()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testIsWrapperFor()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testNativeSQL()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareCallString()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareCallStringIntInt()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareCallStringIntIntInt()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareStatementString()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareStatementStringInt()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareStatementStringIntInt()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareStatementStringIntIntInt()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareStatementStringIntArray()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testPrepareStatementStringStringArray()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testReleaseSavepoint()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testRollback()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testRollbackSavepoint()
	{
		fail("Not yet implemented");
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
	public void testSetAutoCommit()
	{
		fail("Not yet implemented");
	}

	@Test
	public void testSetCatalog()
	{
		fail("Not yet implemented");
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
