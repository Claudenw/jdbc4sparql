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
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;

import java.io.StringReader;
import java.net.URL;
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
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.mock.MockCatalog;
import org.xenei.jdbc4sparql.mock.MockColumn;
import org.xenei.jdbc4sparql.mock.MockSchema;
import org.xenei.jdbc4sparql.mock.MockTableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlVisitor;

public class RemoteSparqlVisitorTest
{

	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private SparqlVisitor sv;

	@Before
	public void setUp() throws Exception
	{
		final SparqlCatalog catalog = new SparqlCatalog(new URL(
				"http://example.com/sparql"), MockCatalog.LOCAL_NAME);
		final MockSchema schema = new MockSchema(catalog);
		catalog.addSchema(schema);
		// create the foo table
		MockTableDef tableDef = new MockTableDef("foo");
		tableDef.add(MockColumn.getBuilder("StringCol", Types.VARCHAR).build());
		tableDef.add(MockColumn.getBuilder("NullableStringCol", Types.VARCHAR)
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(MockColumn.getBuilder("IntCol", Types.INTEGER).build());
		tableDef.add(MockColumn.getBuilder("NullableIntCol", Types.INTEGER)
				.setNullable(DatabaseMetaData.columnNullable).build());
		schema.addTableDef(tableDef);

		// create the bar table
		tableDef = new MockTableDef("bar");
		tableDef.add(MockColumn.getBuilder("BarStringCol", Types.VARCHAR)
				.build());
		tableDef.add(MockColumn
				.getBuilder("BarNullableStringCol", Types.VARCHAR)
				.setNullable(DatabaseMetaData.columnNullable).build());
		tableDef.add(MockColumn.getBuilder("IntCol", Types.INTEGER).build());
		tableDef.add(MockColumn.getBuilder("BarNullableIntCol", Types.INTEGER)
				.setNullable(DatabaseMetaData.columnNullable).build());
		schema.addTableDef(tableDef);

		sv = new SparqlVisitor(catalog);

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
		Assert.assertEquals(9, eLst.size());
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(8, vLst.size());

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
		Assert.assertEquals(5, eLst.size());
		final List<String> filterElements = new ArrayList<String>();
		for (final Element e : eLst)
		{
			if (e instanceof ElementFilter)
			{
				filterElements.add(e.toString());
			}
		}
		Assert.assertEquals(4, filterElements.size());

	}

	@Test
	public void testSpecColParse() throws Exception
	{
		final String query = "SELECT StringCol FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final Element e = q.getQueryPattern();
		Assert.assertTrue(e instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) e;
		final List<Element> eLst = eg.getElements();
		// service and checkTypeF filter only
		Assert.assertEquals(4, eLst.size());
		final List<String> strLst = new ArrayList<String>();
		for (final Element e2 : eLst)
		{
			// there is one mock table entry
			if (e2 instanceof ElementFilter)
			{
				strLst.add(e2.toString());
			}
		}
		Assert.assertTrue(strLst.contains("FILTER checkTypeF(?MockSchema"
				+ NameUtils.SPARQL_DOT + "foo" + NameUtils.SPARQL_DOT
				+ "StringCol)"));

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
		// service and checkTypeF filter only
		Assert.assertEquals(4, eLst.size());
		final List<String> strLst = new ArrayList<String>();
		for (final Element e2 : eLst)
		{
			// there is one mock table entry
			if (e2 instanceof ElementFilter)
			{
				strLst.add(e2.toString());
			}
		}
		Assert.assertTrue(strLst.contains("FILTER checkTypeF(?MockSchema"
				+ NameUtils.SPARQL_DOT + "foo" + NameUtils.SPARQL_DOT
				+ "StringCol)"));
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
