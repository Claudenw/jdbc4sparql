package org.xenei.jdbc4sparql.config;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SDriverTest;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumnDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder;

public class ConfigTest
{
	private RdfCatalog catalog;
	private Model model;
	private J4SConnection connection;
	private J4SConnection connection2;
	private File tmpFile;

	private URL fUrl;

	private void deepCompare( final RdfColumn c1, final RdfColumn c2 )
	{
		Assert.assertEquals(c1.getFQName(), c2.getFQName());
		Assert.assertEquals(c1.getNamespace(), c2.getNamespace());
		Assert.assertEquals(c1.getLocalName(), c2.getLocalName());
		Assert.assertEquals(c1.getQuerySegmentFmt(), c2.getQuerySegmentFmt());
		Assert.assertEquals(c1.getSPARQLName(), c2.getSPARQLName());
		Assert.assertEquals(c1.getSQLName(), c2.getSQLName());
		Assert.assertEquals(c1.getResource(), c1.getResource());
		deepCompare(c1.getColumnDef(), c2.getColumnDef());
	}

	private void deepCompare( final RdfColumnDef c1, final RdfColumnDef c2 )
	{
		Assert.assertEquals(c1.getTypeName(), c2.getTypeName());
		Assert.assertEquals(c1.getDisplaySize(), c2.getDisplaySize());
		Assert.assertEquals(c1.getNullable(), c2.getNullable());
		Assert.assertEquals(c1.getPrecision(), c2.getPrecision());
		Assert.assertEquals(c1.getResource(), c2.getResource());
		Assert.assertEquals(c1.getScale(), c2.getScale());
		Assert.assertEquals(c1.getType(), c2.getType());
	}

	/*
	 * 
	 * final List<String> qs1 = c1.getQuerySegments();
	 * final List<String> qs2 = c2.getQuerySegments();
	 * Assert.assertEquals(qs1.size(), qs2.size());
	 * for (int i = 0; i < qs1.size(); i++)
	 * {
	 * Assert.assertEquals(qs1.get(i), qs2.get(i));
	 * }
	 */
	private void deepCompare( final RdfSchema s1, final RdfSchema s2 )
	{
		Assert.assertEquals(s1.getFQName(), s2.getFQName());
		Assert.assertEquals(s1.getNamespace(), s2.getNamespace());
		Assert.assertEquals(s1.getLocalName(), s2.getLocalName());
		Assert.assertEquals(s1.getName(), s2.getName());
		final Set<RdfTable> s1Tables = s1.getTables();
		final Set<RdfTable> s2Tables = s1.getTables();
		Assert.assertEquals(s1Tables.size(), s2Tables.size());
		for (final Table t1 : s1Tables)
		{
			final Table t2 = s2.getTable(t1.getName());
			Assert.assertNotNull(t2);
			deepCompare((RdfTable) t1, (RdfTable) t2);
		}
	}

	private void deepCompare( final RdfTable t1, final RdfTable t2 )
	{
		Assert.assertEquals(t1.getFQName(), t2.getFQName());
		Assert.assertEquals(t1.getNamespace(), t2.getNamespace());
		Assert.assertEquals(t1.getLocalName(), t2.getLocalName());
		Assert.assertEquals(t1.getColumnCount(), t2.getColumnCount());
		Assert.assertEquals(t1.getQuerySegmentFmt(), t2.getQuerySegmentFmt());
		deepCompare(t1.getTableDef(), t2.getTableDef());
		final Iterator<RdfColumn> cr1 = t1.getColumns();
		final Iterator<RdfColumn> cr2 = t2.getColumns();
		while (cr1.hasNext())
		{
			deepCompare(cr1.next(), cr2.next());
		}

	}

	private void deepCompare( final RdfTableDef t1, final RdfTableDef t2 )
	{
		Assert.assertEquals(t1.getFQName(), t2.getFQName());
		Assert.assertEquals(t1.getNamespace(), t2.getNamespace());
		Assert.assertEquals(t1.getLocalName(), t2.getLocalName());
		Assert.assertEquals(t1.getColumnCount(), t2.getColumnCount());
		final Iterator<ColumnDef> cd1 = t1.getColumnDefs().iterator();
		final Iterator<ColumnDef> cd2 = t2.getColumnDefs().iterator();

		while (cd1.hasNext())
		{
			deepCompare((RdfColumnDef) cd1.next(), (RdfColumnDef) cd2.next());
		}
	}
	
	private void deepCompare( final Catalog c1, final Catalog c2 )
	{
		Assert.assertEquals( c1.getName(), c2.getName() );
		Set<Schema> s1 = (Set<Schema>) c1.getSchemas();
		Set<Schema> s2 = (Set<Schema>) c2.getSchemas();
		Assert.assertEquals( s1.size(), s2.size() );
		Iterator<Schema> iterS1 = s1.iterator();
		Iterator<Schema> iterS2 = s2.iterator();
		while (iterS1.hasNext())
		{
			Schema i1 = iterS1.next();
			Schema i2 = iterS2.next();
			Assert.assertTrue( i1 instanceof RdfSchema);
			Assert.assertTrue( i2 instanceof RdfSchema);
			deepCompare( (RdfSchema)i1, (RdfSchema)i2 );
		}
	}

	@Before
	public void setup()
	{
		fUrl = J4SDriverTest.class.getResource("./J4SStatementTest.ttl"); // /org/xenei/jdbc4sparql/J4SDriverTest.ttl");

		model = ModelFactory.createDefaultModel();
		model.read(fUrl.toString(), "TURTLE");

		catalog = new RdfCatalog.Builder().setName("SimpleSparql")
				.setLocalModel(model).build(model);

	}

	@After
	public void teardown() throws SQLException
	{
		if (connection != null)
		{
			connection.close();
		}
		if (connection2 != null)
		{
			connection2.close();
		}
		if (tmpFile != null)
		{
			tmpFile.delete();
		}
		catalog.close();
		model.close();
	}

	@Test
	public void testModelRoundTrip() throws Exception
	{
		new RdfSchema.Builder().setCatalog(catalog).setName("builderTest")
				.build(model);

		final J4SUrl url = new J4SUrl("jdbc:j4s?catalog=test&type=turtle&builder=org.xenei.jdbc4sparql.sparql.builders.SimpleNullableBuilder:"
		+ fUrl.toString());
		final J4SDriver driver = new J4SDriver();
		connection = new J4SConnection(driver, url, new Properties());
		tmpFile = File.createTempFile("CfgTst", ".zip");
				
		FileOutputStream fos = new FileOutputStream( tmpFile );
		connection.saveConfig(fos);
		fos.close();

		final J4SUrl url2 = new J4SUrl("jdbc:J4S:file:"+tmpFile.getCanonicalPath());
		
		
		connection2 = new J4SConnection(driver, url2, new Properties());
		
		Map<String,Catalog> cats = connection.getCatalogs();
		Map<String,Catalog> cats2 = connection2.getCatalogs();
		
		Assert.assertEquals( cats.size(), cats2.size() );
		Iterator<Catalog> iterCat = cats.values().iterator();
		Iterator<Catalog> iterCat2 = cats2.values().iterator();
		
		while (iterCat.hasNext())
		{
			deepCompare( iterCat.next(), iterCat2.next() );
		}

	}

}
