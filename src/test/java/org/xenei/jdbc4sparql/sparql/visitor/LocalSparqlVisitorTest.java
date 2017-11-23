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

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.E_Bound;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_LogicalNot;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementBind;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementOptional;
import org.apache.jena.sparql.syntax.ElementPathBlock;

public class LocalSparqlVisitorTest extends AbstractSparqlVisitorTest {

	
	@Test
	public void testInnerJoinParse() throws Exception {
		final String[] colNames = {
				FOO_TABLE_NAME.getColumnName("StringCol").getGUID(),
				FOO_TABLE_NAME.getColumnName("NullableStringCol").getGUID(),
				FOO_TABLE_NAME.getColumnName("IntCol").getGUID(),
				FOO_TABLE_NAME.getColumnName("NullableIntCol").getGUID(),
				BAR_TABLE_NAME.getColumnName("BarStringCol").getGUID(),
				BAR_TABLE_NAME.getColumnName("BarNullableStringCol").getGUID(),
				BAR_TABLE_NAME.getColumnName("BarIntCol").getGUID(),
				BAR_TABLE_NAME.getColumnName("NullableIntCol").getGUID()				
		};

		final Query q = getQuery("SELECT * FROM foo inner join bar using (NullableIntCol)");

		tests.put(ElementBind.class, 8);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals( 8, vLst.size());
		for (final Var v : vLst) {
			Assert.assertTrue("Missing: "+v.getName(), Arrays.asList(colNames).contains(v.getName()));
		}

		for (final Element el : results.get(ElementBind.class).lst) {
			final ElementBind bind = (ElementBind) el;
			Assert.assertTrue(q.getProjectVars().contains(bind.getVar()));
		}
	}

	@Test
	public void testMethodParse() throws Exception {
		final Query q = getQuery("SELECT count( IntCol ) FROM foo");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		final VarExprList vLst = q.getProject();
		Assert.assertEquals(1, vLst.getExprs().size());

	}

	@Test
	public void testMethodWithAliasParse() throws Exception {
		final Query q = getQuery("SELECT count( IntCol ) AS bar FROM foo");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		Assert.assertEquals(1, q.getProject().size());
		Assert.assertEquals(1, q.getProjectVars().size());
		Assert.assertEquals( VirtualTable.getDefaultName().getColumnName( "bar").getGUID(), q.getProjectVars().get(0).getVarName());

	}

	@Test
	public void testNoColParse() throws Exception {
		final String[] colNames = {
				new ColumnName( CATALOG_NAME, SCHEMA_NAME, "foo", "StringCol").getGUID(),
				new ColumnName( CATALOG_NAME, SCHEMA_NAME, "foo", "NullableStringCol").getGUID(),
				new ColumnName( CATALOG_NAME, SCHEMA_NAME, "foo", "IntCol").getGUID(),
				new ColumnName( CATALOG_NAME, SCHEMA_NAME, "foo", "NullableIntCol").getGUID()
		};

		final Query q = getQuery("SELECT * FROM foo");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(4, vLst.size());
		for (final Var v : vLst) {
			Assert.assertTrue(Arrays.asList(colNames).contains(v.getName()));
		}

		for (final Element el : results.get(ElementBind.class).lst) {
			final ElementBind bind = (ElementBind) el;
			Assert.assertTrue(q.getProjectVars().contains(bind.getVar()));
		}
	}

	@Test
	public void testSpecColParse() throws Exception {
		final Query q = getQuery("SELECT StringCol FROM foo");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		ColumnName strCol = new ColumnName( CATALOG_NAME, SCHEMA_NAME, "foo", "StringCol");
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc(strCol.getGUID()), vLst.get(0));

		// 1 required column
		final ElementBind eb = (ElementBind) results.get(ElementBind.class).lst
				.get(0);
		Assert.assertEquals(strCol.getGUID(), eb.getVar().getName());
	}

	@Test
	public void testSpecColWithAliasParse() throws Exception {
		final Query q = getQuery("SELECT StringCol AS bar FROM foo");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		ColumnName barName = new ColumnName( CATALOG_NAME, SCHEMA_NAME, "foo", "bar");
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc( barName.getGUID()), vLst.get(0));

		final ElementBind eb = (ElementBind) results.get(ElementBind.class).lst
				.get(0);
		Assert.assertEquals(barName.getGUID(), eb.getVar().getName());
	}

	@Test
	public void testSpecColWithEqnParse() throws Exception {
		final Query q = getQuery("SELECT StringCol FROM foo WHERE StringCol != 'baz'");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		// 1 required column

		final ElementBind eb = (ElementBind) results.get(ElementBind.class).lst
				.get(0);
		ColumnName strgCol = new ColumnName( CATALOG_NAME, SCHEMA_NAME, "foo", "StringCol");
		Assert.assertEquals(strgCol.getGUID(), eb.getVar().getName());

		// should be the last one
		final Expr expr = ((ElementFilter) results.get(ElementFilter.class).lst
				.get(1)).getExpr();
		Assert.assertTrue(expr instanceof E_NotEquals);
		final E_NotEquals expr2 = (E_NotEquals) expr;
		Assert.assertEquals(strgCol.getGUID(),
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("baz",
				((NodeValueString) (expr2.getArg2())).asString());

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc(strgCol.getGUID()), vLst.get(0));

	}

	@Test
	public void testTableAliasParse() throws Exception {
		final Query q = getQuery("SELECT StringCol FROM foo bar WHERE StringCol != 'baz'");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc( new ColumnName( CATALOG_NAME, SCHEMA_NAME, "bar", "StringCol").getGUID()), vLst.get(0));

		final ElementBind eb = (ElementBind) results.get(ElementBind.class).lst
				.get(0);
		Assert.assertEquals( new ColumnName( CATALOG_NAME, SCHEMA_NAME, "bar", "StringCol").getGUID(), eb.getVar().getName());

		// should be the last one
		final Expr expr = ((ElementFilter) results.get(ElementFilter.class).lst
				.get(1)).getExpr();
		Assert.assertTrue(expr instanceof E_NotEquals);
		final E_NotEquals expr2 = (E_NotEquals) expr;
		Assert.assertEquals( new ColumnName( CATALOG_NAME, SCHEMA_NAME, "bar", "StringCol").getGUID(),
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("baz",
				((NodeValueString) (expr2.getArg2())).asString());

		verifyTable(results.get(ElementPathBlock.class), tableName.getGUID());
	}

	@Test
	public void testTwoTableJoin() throws Exception {
		final ColumnName[] columnNames = {
				FOO_TABLE_NAME.getColumnName("IntCol"),
				FOO_TABLE_NAME.getColumnName("StringCol"),
				FOO_TABLE_NAME.getColumnName("NullableStringCol"),
				FOO_TABLE_NAME.getColumnName("NullableIntCol"),
				BAR_TABLE_NAME.getColumnName("BarIntCol"),
				BAR_TABLE_NAME.getColumnName("BarStringCol"),
				BAR_TABLE_NAME.getColumnName("BarNullableStringCol"),
				BAR_TABLE_NAME.getColumnName("NullableIntCol")
		};
		final Query q = getQuery("SELECT * FROM foo, bar WHERE foo.IntCol = bar.BarIntCol");

		tests.put(ElementBind.class, columnNames.length);
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(columnNames.length, vLst.size());
		for (final ColumnName cn: columnNames) {
			
			Assert.assertTrue("missing " + cn.getSPARQLName(),
					vLst.contains(Var.alloc(cn.getGUID())));
		}

		final Expr expr = ((ElementFilter) results.get(ElementFilter.class).lst
				.get(2)).getExpr();
		Assert.assertTrue(expr instanceof E_Equals);
		final E_Equals expr2 = (E_Equals) expr;
		Assert.assertEquals(FOO_TABLE_NAME.getColumnName("IntCol").getGUID(),
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals(BAR_TABLE_NAME.getColumnName("BarIntCol").getGUID(),
				((ExprVar) (expr2.getArg2())).getVarName());
	}

	@Test
	public void testCountNullableColumn() throws Exception {
		final Query q = getQuery("select count(NullableIntCol) from foo");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

	}

	@Test
	public void testColumnByNotNull() throws Exception {
		final Query q = getQuery("select IntCol from foo WHERE NullableIntCol IS NULL");

		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		results = validate(q, tests);

		Expr expr = ((ElementFilter) results.get(ElementFilter.class).lst
				.get(1)).getExpr();

		Assert.assertTrue(expr instanceof E_LogicalNot);
		expr = ((E_LogicalNot) expr).getArg();
		Assert.assertTrue(expr instanceof E_Bound);
		Assert.assertEquals( FOO_TABLE_NAME.getColumnName("NullableIntCol").getGUID(),
				((E_Bound) expr).getArg().asVar().getName());

	}
}
