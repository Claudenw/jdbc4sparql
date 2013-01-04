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
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;

import java.io.StringReader;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.xenei.jdbc4sparql.mock.MockCatalog;
import org.xenei.jdbc4sparql.mock.MockColumn;
import org.xenei.jdbc4sparql.mock.MockSchema;
import org.xenei.jdbc4sparql.mock.MockTableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlVisitor;

public class SparqlVisitorTest
{

	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private SparqlVisitor sv;

	@Before
	public void setUp() throws Exception
	{
		final SparqlCatalog catalog = new SparqlCatalog(MockCatalog.NS, null,
				MockCatalog.LOCAL_NAME);
		final MockSchema schema = new MockSchema(catalog);
		catalog.addSchema(schema);
		// create the foo table
		MockTableDef tableDef = new MockTableDef("foo");
		tableDef.add(new MockColumn("StringCol", Types.VARCHAR));
		tableDef.add(new MockColumn("NullableStringCol", Types.VARCHAR)
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MockColumn("IntCol", Types.INTEGER));
		tableDef.add(new MockColumn("NullableIntCol", Types.INTEGER)
				.setNullable(DatabaseMetaData.columnNullable));
		schema.addTableDef(tableDef);

		// creae the var table
		tableDef = new MockTableDef("bar");
		tableDef.add(new MockColumn("BarStringCol", Types.VARCHAR));
		tableDef.add(new MockColumn("BarNullableStringCol", Types.VARCHAR)
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(new MockColumn("IntCol", Types.INTEGER));
		tableDef.add(new MockColumn("BarNullableIntCol", Types.INTEGER)
				.setNullable(DatabaseMetaData.columnNullable));
		schema.addTableDef(tableDef);

		sv = new SparqlVisitor(catalog);

	}

	@Test
	public void testNoColParse() throws Exception
	{
		final String query = "SELECT * FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(4, vLst.size());
		Assert.assertTrue(q.getQueryPattern() instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) q.getQueryPattern();
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(6, eLst.size());
		final List<String> bindElements = new ArrayList<String>();
		for (final Element e : eLst)
		{
			if (e instanceof ElementBind)
			{
				bindElements.add(e.toString());
			}
		}
		Assert.assertEquals(4, bindElements.size());
		Assert.assertTrue(bindElements.contains(String.format(
				"BIND(?MockSchema%1$sfoo%1$s%2$s AS ?%2$s)",
				SparqlParser.SPARQL_DOT, "StringCol")));
		Assert.assertTrue(bindElements.contains(String.format(
				"BIND(?MockSchema%1$sfoo%1$s%2$s AS ?%2$s)",
				SparqlParser.SPARQL_DOT, "NullableStringCol")));
		Assert.assertTrue(bindElements.contains(String.format(
				"BIND(?MockSchema%1$sfoo%1$s%2$s AS ?%2$s)",
				SparqlParser.SPARQL_DOT, "IntCol")));
		Assert.assertTrue(bindElements.contains(String.format(
				"BIND(?MockSchema%1$sfoo%1$s%2$s AS ?%2$s)",
				SparqlParser.SPARQL_DOT, "NullableIntCol")));
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
		Assert.assertEquals(1, eLst.size());
		Assert.assertTrue(eLst.get(0) instanceof ElementFilter);
		Assert.assertEquals("FILTER ( ?MockSchema" + SparqlParser.SPARQL_DOT
				+ "foo" + SparqlParser.SPARQL_DOT + "StringCol != \"baz\" )",
				eLst.get(0).toString());
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

	}

	@Test
	@Ignore( "This only remains as a pattern for a complete test -- Mock can not support this test" )
	public void testTwoTableJoin() throws Exception
	{
		final String query = "SELECT * FROM foo, bar WHERE foo.IntCol = bar.IntCol";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final Element e = q.getQueryPattern();
		Assert.assertTrue(e instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) e;
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(17, eLst.size());
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(8, vLst.size());
	}
}
