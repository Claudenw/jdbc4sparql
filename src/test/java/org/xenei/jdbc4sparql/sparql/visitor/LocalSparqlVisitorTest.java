/*
 * This file is part of jdbc4sparql jsqlparser implementation.
 *
 * jdbc4sparql jsqlparser implementation is free software: you can redistribute
 * it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * jdbc4sparql jsqlparser implementation is distributed in the hope that it will
 * be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with jdbc4sparql jsqlparser implementation. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.xenei.jdbc4sparql.sparql.visitor;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.LoggingConfig;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ColumnName;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlVisitor;

public class LocalSparqlVisitorTest
{
	private Map<String, Catalog> catalogs;
	private SparqlParser parser;
	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private SparqlVisitor sv;

	@Before
	public void setUp() throws Exception
	{
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("com.hp.hpl.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		final Model model = ModelFactory.createDefaultModel();
		final Model localModel = ModelFactory.createDefaultModel();
		final RdfCatalog catalog = new RdfCatalog.Builder()
		.setLocalModel(localModel).setName("testCatalog").build(model);

		final RdfSchema schema = new RdfSchema.Builder().setCatalog(catalog)
				.setName("testSchema").build(model);

		// create the foo table
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(
				MetaCatalogBuilder.getNonNullStringBuilder().build(
						model))
						.addColumnDef(
								MetaCatalogBuilder.getNullStringBuilder().build(model))
								.addColumnDef(
										MetaCatalogBuilder.getNonNullIntBuilder().build(model))
										.addColumnDef(
												MetaCatalogBuilder.getNullIntBuilder().build(model))
												.build(model);

		RdfTable.Builder bldr = new RdfTable.Builder().setTableDef(tableDef)
				.setColumn(0, "StringCol").setColumn(1, "NullableStringCol")
				.setColumn(2, "IntCol").setColumn(3, "NullableIntCol")
				.setName("foo").setSchema(schema)
				.addQuerySegment("%1$s a <http://example.com/foo> . ");
		bldr.getColumn(0).addQuerySegment(
				"%1$s <http://example.com/zero> %2$s .");
		bldr.getColumn(1).addQuerySegment(
				"%1$s <http://example.com/one> %2$s . ");
		bldr.getColumn(2).addQuerySegment(
				"%1$s <http://example.com/two> %2$s . ");
		bldr.getColumn(3).addQuerySegment(
				"%1$s <http://example.com/three> %2$s .");
		bldr.build(model);

		bldr = new RdfTable.Builder().setTableDef(tableDef)
				.setColumn(0, "BarStringCol")
				.setColumn(1, "BarNullableStringCol")
				.setColumn(2, "BarIntCol")
				// must be NullableIntCol for inner join test
				.setColumn(3, "NullableIntCol").setName("bar")
				.setSchema(schema)
				.addQuerySegment("%1$s a <http://example.com/bar> . ");
		bldr.getColumn(0).addQuerySegment(
				"%1$s <http://example.com/zero> %2$s . ");
		bldr.getColumn(1).addQuerySegment(
				"%1$s <http://example.com/one> %2$s . ");
		bldr.getColumn(2).addQuerySegment(
				"%1$s <http://example.com/two> %2$s . ");
		bldr.getColumn(3).addQuerySegment(
				"%1$s <http://example.com/three> %2$s . ");
		bldr.build(model);

		parser = new SparqlParserImpl();
		sv = new SparqlVisitor(catalogs, parser, catalog, schema);

	}

	@Test
	public void testInnerJoinParse() throws Exception
	{
		final String query = "SELECT * FROM foo inner join bar using (NullableIntCol)";
		final Statement stmt = parserManager.parse(new StringReader(query));

		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final Element e = q.getQueryPattern();
		Assert.assertTrue(e instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) e;
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(2, eLst.size()); // 2 tables
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(7, vLst.size());

	}

	@Test
	public void testNoColParse() throws Exception
	{
		final String[] colNames = { "StringCol", "NullableStringCol", "IntCol",
		"NullableIntCol" };
		final String query = "SELECT * FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(4, vLst.size());
		for (final String colName : colNames)
		{
			Assert.assertTrue(String.format("missing var %s", colName),
					vLst.contains(Var.alloc(colName)));
		}
		Assert.assertTrue(q.getQueryPattern() instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) q.getQueryPattern();
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(1, eLst.size()); // table
	}

	@Test
	public void testSpecColParse() throws Exception
	{
		final String query = "SELECT StringCol FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

	}

	@Test
	public void testSpecColWithAliasParse() throws Exception
	{
		final String query = "SELECT StringCol AS bar FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("bar"), vLst.get(0));

	}

	@Test
	public void testSpecColWithEqnParse() throws Exception
	{
		final String query = "SELECT StringCol FROM foo WHERE StringCol != 'baz'";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final Element e = q.getQueryPattern();
		Assert.assertTrue(e instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) e;
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(2, eLst.size());
		final List<String> strLst = new ArrayList<String>();
		for (final Element e2 : eLst)
		{
			// there is one mock table entry
			if (e2 instanceof ElementFilter)
			{
				strLst.add(e2.toString());
			}
		}
		final String value = "FILTER ( ?StringCol != \"baz\" )";
		Assert.assertTrue(strLst.contains(value));

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

	}

	@Test
	public void testTableAliasParse() throws Exception
	{
		final String query = "SELECT StringCol FROM foo bar WHERE StringCol != 'baz'";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final Element e = q.getQueryPattern();
		Assert.assertTrue(e instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) e;
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(2, eLst.size());
		final List<String> strLst = new ArrayList<String>();
		for (final Element e2 : eLst)
		{
			// there is one mock table entry
			if (e2 instanceof ElementFilter)
			{
				strLst.add(e2.toString());
			}
		}
		final String value = "FILTER ( ?StringCol != \"baz\" )";
		Assert.assertTrue(strLst.contains(value));

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

	}

	@Test
	public void testTwoTableJoin() throws Exception
	{
		final String[] columnNames = {
				"?" + new ColumnName(null, "foo", "IntCol").getSPARQLName(),
				"?" + new ColumnName(null, "foo", "StringCol").getSPARQLName(),
				"?"
						+ new ColumnName(null, "foo", "NullableStringCol")
				.getSPARQLName(),
				"?" + new ColumnName(null, "foo", "IntCol"),
				"?"
						+ new ColumnName(null, "foo", "NullableIntCol")
				.getSPARQLName(),
				"?"
						+ new ColumnName(null, "bar", "BarStringCol")
				.getSPARQLName(),
				"?"
						+ new ColumnName(null, "bar", "BarNullableStringCol")
				.getSPARQLName(),
				"?" + new ColumnName(null, "bar", "BarIntCol").getSPARQLName(),
				"?"
						+ new ColumnName(null, "bar", "NullableIntCol")
				.getSPARQLName() };
		final String query = "SELECT * FROM foo, bar WHERE foo.IntCol = bar.BarIntCol";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final Element e = q.getQueryPattern();
		Assert.assertTrue(e instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) e;
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(3, eLst.size()); // foo set, bar set and 1 filter
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(8, vLst.size());
		final List<String> colList = Arrays.asList(columnNames);
		for (int i = 0; i < vLst.size(); i++)
		{
			Assert.assertTrue(
					String.format("Column %s not expected", vLst.get(i)),
					colList.contains(vLst.get(i).toString()));
		}
	}
}
