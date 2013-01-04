package org.xenei.jdbc4sparql;


import org.junit.Assert;
import org.junit.Test;

public class J4SURLTest
{
	@Test
	public void testSimpleURL()
	{
		J4SURL url; 
		url = new J4SURL( "jdbc:j4s:http://example.com/test.file" );
		Assert.assertNull( url.getCatalog() );
		Assert.assertNull( url.getBuilder() );
		Assert.assertEquals( "http://example.com/test.file", url.getEndpoint().toString());
		
		url = new J4SURL( "jdbc:j4s:file:///test.file" );
		Assert.assertNull( url.getCatalog() );
		Assert.assertNull( url.getBuilder() );
		Assert.assertEquals( "file:///test.file", url.getEndpoint().toString());
		
		url = new J4SURL( "jdbc:j4s:ftp://example.com/test.file" );
		Assert.assertNull( url.getCatalog() );
		Assert.assertNull( url.getBuilder() );
		Assert.assertEquals( "ftp://example.com/test.file", url.getEndpoint().toString());
	}

	@Test
	public void testCatalogURL()
	{
		J4SURL url; 
		url = new J4SURL( "jdbc:j4s?catalog=foo:http://example.com/test.file" );
		Assert.assertEquals( "foo", url.getCatalog() );
		Assert.assertNull( url.getBuilder() );
		Assert.assertEquals( "http://example.com/test.file", url.getEndpoint().toString());
		
		url = new J4SURL( "jdbc:j4s?catalog=foo:file:///test.file" );
		Assert.assertEquals( "foo", url.getCatalog() );
		Assert.assertNull( url.getBuilder() );
		Assert.assertEquals( "file:///test.file", url.getEndpoint().toString());
		
		url = new J4SURL( "jdbc:j4s?catalog=foo:ftp://example.com/test.file" );
		Assert.assertEquals( "foo", url.getCatalog() );
		Assert.assertNull( url.getBuilder() );
		Assert.assertEquals( "ftp://example.com/test.file", url.getEndpoint().toString());
	}
	
	@Test
	public void testCatalogParserURL()
	{
		J4SURL url; 
		url = new J4SURL( "jdbc:j4s?catalog=foo&builder=bar:http://example.com/test.file" );
		Assert.assertEquals( "foo", url.getCatalog() );
		Assert.assertEquals( "bar", url.getBuilder() );
		Assert.assertEquals( "http://example.com/test.file", url.getEndpoint().toString());
		
		url = new J4SURL( "jdbc:j4s?catalog=foo&builder=bar:file:///test.file" );
		Assert.assertEquals( "foo", url.getCatalog() );
		Assert.assertEquals( "bar", url.getBuilder() );
		Assert.assertEquals( "file:///test.file", url.getEndpoint().toString());
		
		url = new J4SURL( "jdbc:j4s?catalog=foo&builder=bar:ftp://example.com/test.file" );
		Assert.assertEquals( "foo", url.getCatalog() );
		Assert.assertEquals( "bar", url.getBuilder() );
		Assert.assertEquals( "ftp://example.com/test.file", url.getEndpoint().toString());
	}
}
