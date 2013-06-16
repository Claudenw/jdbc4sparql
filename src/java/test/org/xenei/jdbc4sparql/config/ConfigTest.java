package org.xenei.jdbc4sparql.config;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.builders.SimpleBuilder;
import org.xenei.jdbc4sparql.sparql.builders.SimpleNullableBuilder;

public class ConfigTest
{
	private class MultiQueryStatementBuilder extends SimpleBuilder
	{
		@Override
		protected void addColumnDefs( final SparqlCatalog catalog,
				final SparqlTableDef tableDef, final Resource tName )
		{
			final List<QuerySolution> solns = catalog.executeQuery(String
					.format(SimpleBuilder.COLUMN_QUERY, tName));
			final SparqlColumnDef.Builder builder = new SparqlColumnDef.Builder();
			for (final QuerySolution soln : solns)
			{
				final Resource cName = soln.getResource("cName");
				builder.addQuerySegment("Query Segment 1")
						.addQuerySegment("Query Segment 2")
						.addQuerySegment("Query Segment 3")

						.setNamespace(cName.getNameSpace())
						.setLocalName(cName.getLocalName())
						.setType(Types.VARCHAR)
						.setNullable(DatabaseMetaData.columnNullable);
				tableDef.add(builder.build());
			}
		}

		@Override
		public Set<TableDef> getTableDefs( final SparqlCatalog catalog )
		{
			final HashSet<TableDef> retval = new HashSet<TableDef>();
			final List<QuerySolution> solns = catalog
					.executeQuery(SimpleBuilder.TABLE_QUERY);
			for (final QuerySolution soln : solns)
			{
				final Resource tName = soln.getResource("tName");
				final SparqlTableDef tableDef = new SparqlTableDef(
						tName.getNameSpace(), tName.getLocalName(),
						"Query Segment 1", null);
				tableDef.addQuerySegment("Query Segment 2");
				tableDef.addQuerySegment("Query Segment 3");
				addColumnDefs(catalog, tableDef, tName);
				retval.add(tableDef);
			}
			return retval;
		}

	}

	private static final String CAT_NS = "http://example.com/jdbc4sparql/meta/catalog#";
	private static final String NS = "http://example.com/jdbc4sparql#";
	private SparqlCatalog catalog;
	private Model model;
	private SchemaBuilder builder;
	private Dataset dataset;

	private URL fUrl;

	private void deepCompare( final SparqlCatalog cat1, final SparqlCatalog cat2 )
	{
		Assert.assertEquals(cat1.getFQName(), cat2.getFQName());
		Assert.assertEquals(cat1.getNamespace(), cat2.getNamespace());
		Assert.assertEquals(cat1.getLocalName(), cat2.getLocalName());
		Assert.assertEquals(cat1.isService(), cat2.isService());
		Assert.assertEquals(cat1.getServiceNode(), cat2.getServiceNode());
		final Set<Schema> cat1Schemas = cat1.getSchemas();
		final Set<Schema> cat2Schemas = cat2.getSchemas();
		Assert.assertEquals(cat1Schemas.size(), cat2Schemas.size());
		for (final Schema s1 : cat1Schemas)
		{
			final Schema s2 = cat2.getSchema(s1.getLocalName());
			Assert.assertNotNull(s2);
			deepCompare((SparqlSchema) s1, (SparqlSchema) s2);
		}
	}

	private void deepCompare( final SparqlColumnDef c1, final SparqlColumnDef c2 )
	{
		Assert.assertEquals(c1.getFQName(), c2.getFQName());
		Assert.assertEquals(c1.getNamespace(), c2.getNamespace());
		Assert.assertEquals(c1.getLocalName(), c2.getLocalName());
		Assert.assertEquals(c1.getId(), c2.getId());
		final List<String> qs1 = c1.getQuerySegments();
		final List<String> qs2 = c2.getQuerySegments();
		Assert.assertEquals(qs1.size(), qs2.size());
		for (int i = 0; i < qs1.size(); i++)
		{
			Assert.assertEquals(qs1.get(i), qs2.get(i));
		}
	}

	private void deepCompare( final SparqlSchema s1, final SparqlSchema s2 )
	{
		Assert.assertEquals(s1.getFQName(), s2.getFQName());
		Assert.assertEquals(s1.getNamespace(), s2.getNamespace());
		Assert.assertEquals(s1.getLocalName(), s2.getLocalName());
		final Set<Table> s1Tables = s1.getTables();
		final Set<Table> s2Tables = s1.getTables();
		Assert.assertEquals(s1Tables.size(), s2Tables.size());
		for (final Table t1 : s1Tables)
		{
			final Table t2 = s2.getTable(t1.getLocalName());
			Assert.assertNotNull(t2);
			deepCompare((SparqlTable) t1, (SparqlTable) t2);
		}
	}

	private void deepCompare( final SparqlTable t1, final SparqlTable t2 )
	{
		Assert.assertEquals(t1.getFQName(), t2.getFQName());
		Assert.assertEquals(t1.getNamespace(), t2.getNamespace());
		Assert.assertEquals(t1.getLocalName(), t2.getLocalName());
		Assert.assertEquals(t1.getColumnCount(), t2.getColumnCount());
		final List<ColumnDef> t1Cols = t1.getColumnDefs();
		final List<ColumnDef> t2Cols = t2.getColumnDefs();
		Assert.assertEquals(t1Cols.size(), t2Cols.size());
		Assert.assertEquals(t1.getColumnCount(), t1Cols.size());
		for (int i = 0; i < t1Cols.size(); i++)
		{
			deepCompare((SparqlColumnDef) t1Cols.get(i),
					(SparqlColumnDef) t2Cols.get(i));
		}
		final SparqlTableDef td1 = t1.getTableDef();
		final SparqlTableDef td2 = t2.getTableDef();
		final List<String> sq1 = td1.getQuerySegments();
		final List<String> sq2 = td2.getQuerySegments();
		Assert.assertEquals(sq1.size(), sq2.size());
		for (int i = 0; i < sq1.size(); i++)
		{
			Assert.assertEquals(sq1.get(i), sq2.get(i));
		}

	}

	@Before
	public void setup()
	{
		fUrl = J4SDriverTest.class.getResource("./J4SStatementTest.ttl"); // /org/xenei/jdbc4sparql/J4SDriverTest.ttl");

		model = ModelFactory.createDefaultModel();
		model.read(fUrl.toString(), "TURTLE");

		catalog = new SparqlCatalog(ConfigTest.CAT_NS, model, "SimpleSparql");
		dataset = DatasetFactory.createMem();
	}

	@After
	public void teardown()
	{
		dataset.close();
	}

	@Test
	public void testModelReload() throws IOException, SQLException
	{
		final String[][] results = {
				{ "[StringCol]=FooString",
						"[NullableStringCol]=FooNullableFooString",
						"[NullableIntCol]=6", "[IntCol]=5",
						"[type]=http://example.com/jdbc4sparql#fooTable" },
				{ "[StringCol]=Foo2String", "[NullableStringCol]=null",
						"[NullableIntCol]=null", "[IntCol]=5",
						"[type]=http://example.com/jdbc4sparql#fooTable" } };

		final SparqlSchema schema = new SparqlSchema(catalog, ConfigTest.NS,
				"builderTest");
		catalog.addSchema(schema);
		builder = new SimpleNullableBuilder();
		schema.addTableDefs(builder.getTables(catalog));
		final ConfigSerializer cs = new ConfigSerializer();
		cs.add(catalog);
		final File f = File.createTempFile("cfgTst", ".ttl");
		cs.save(new ModelWriter(f));

		final J4SUrl url = new J4SUrl("jdbc:J4S:"
				+ f.toURI().normalize().toASCIIString());

		final J4SDriver driver = new J4SDriver();
		final J4SConnection connection = new J4SConnection(driver, url, null);
		connection.getModelReader().read(model);
		connection.setCatalog(catalog.getLocalName());
		connection.setSchema(schema.getLocalName());

		// get the column names.
		final ResultSet rs = connection.getMetaData().getColumns(
				connection.getCatalog(), connection.getSchema(), "fooTable",
				null);
		final List<String> colNames = new ArrayList<String>();
		while (rs.next())
		{
			colNames.add(rs.getString(4));
		}

		final Statement stmt = connection.createStatement();
		stmt.execute("select * from fooTable");
		final ResultSet rset = stmt.getResultSet();
		int i = 0;
		while (rset.next())
		{
			final List<String> lst = Arrays.asList(results[i]);
			for (final String colName : colNames)
			{
				lst.contains(String.format("[%s]=%s", colName,
						rset.getString(colName)));
			}
			i++;
		}
		Assert.assertEquals(2, i);
		rset.close();
		connection.close();
	}

	@Test
	public void testRoundTrip() throws IOException
	{
		final SparqlSchema schema = new SparqlSchema(catalog, ConfigTest.NS,
				"builderTest");
		catalog.addSchema(schema);
		builder = new SimpleBuilder();
		schema.addTableDefs(builder.getTables(catalog));

		ConfigSerializer cs = new ConfigSerializer();
		cs.add(catalog);
		final ByteArrayOutputStream boas = new ByteArrayOutputStream();
		cs.save(new ModelWriter(boas));
		cs.save(new ModelWriter(System.out));
		cs = new ConfigSerializer();
		cs.getLoader().read(new ByteArrayInputStream(boas.toByteArray()), "",
				"TTL");
		final Dataset dataset = DatasetFactory.createMem();
		final SparqlCatalog cat2 = cs.getCatalog(dataset, catalog.getFQName());
		deepCompare(catalog, cat2);
		dataset.close();
	}

	@Test
	public void testRoundTripMultiQueryStatement() throws IOException
	{
		final SparqlSchema schema = new SparqlSchema(catalog, ConfigTest.NS,
				"builderTest");
		catalog.addSchema(schema);
		builder = new MultiQueryStatementBuilder();
		schema.addTableDefs(builder.getTables(catalog));

		ConfigSerializer cs = new ConfigSerializer();
		cs.add(catalog);
		final ByteArrayOutputStream boas = new ByteArrayOutputStream();

		cs.save(new ModelWriter(boas));
		cs.save(new ModelWriter(System.out));
		cs = new ConfigSerializer();
		cs.getLoader().read(new ByteArrayInputStream(boas.toByteArray()), "",
				"TTL");

		final SparqlCatalog cat2 = cs.getCatalog(dataset, catalog.getFQName());
		deepCompare(catalog, cat2);

	}
}
