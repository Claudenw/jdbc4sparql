package org.xenei.jdbc4sparql.config;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SDriverTest;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jdbc4sparql.iface.ColumnDef;
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
	private SchemaBuilder builder;
	private Dataset dataset;

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

	@Before
	public void setup()
	{
		fUrl = J4SDriverTest.class.getResource("./J4SStatementTest.ttl"); // /org/xenei/jdbc4sparql/J4SDriverTest.ttl");

		model = ModelFactory.createDefaultModel();
		model.read(fUrl.toString(), "TURTLE");

		catalog = new RdfCatalog.Builder().setName("SimpleSparql")
				.setLocalModel(model).build(model);

		dataset = DatasetFactory.createMem();
	}

	@After
	public void teardown()
	{
		dataset.close();
	}

	@Test
	public void testModelRoundTrip() throws Exception
	{
		new RdfSchema.Builder().setCatalog(catalog).setName("builderTest")
				.build(model);

		// builder = new SimpleNullableBuilder();
		builder = new SimpleBuilder();

		// schema.addTables(builder.getTables(catalog));

		final J4SUrl url = new J4SUrl("jdbc:J4S:"
				+ fUrl.toURI().normalize().toASCIIString());
		final J4SDriver driver = new J4SDriver();
		final J4SConnection connection = new J4SConnection(driver, url, null);
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		connection.saveConfig(baos);
		final DatabaseMetaData dmd = connection.getMetaData();
		final ResultSet rs = dmd.getColumns(null, null, null, null);
		rs.beforeFirst();
		final ResultSetMetaData rsmd = rs.getMetaData();

		new ByteArrayInputStream(baos.toByteArray());
		final J4SConnection connection2 = new J4SConnection(driver, url, null);
		final DatabaseMetaData dmd2 = connection2.getMetaData();
		final ResultSet rs2 = dmd2.getColumns(null, null, null, null);
		rs2.beforeFirst();

		while (rs.next())
		{
			Assert.assertTrue(rs2.next());
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
			{
				Assert.assertEquals(rs.getString(i), rs2.getString(i));
			}
		}

	}

}
