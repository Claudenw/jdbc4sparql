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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlVisitor;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;

public class RemoteSparqlVisitorTest extends AbstractSparqlVisitorTest {

	private static final String SERVICE_URI = "http://example.com/sparql";

	@Override
	@Before
	public void setup() throws Exception {
		super.setup();
		final Property p = model.createProperty(
				"http://org.xenei.jdbc4sparql/entity/Catalog#",
				"sparqlEndpoint");

		catalog.getResource().addProperty(p, SERVICE_URI);
		assertTrue("Catalog was not set to service", catalog.isService());
		// recreate the visitor to handle service catalog
		sv = new SparqlVisitor(catalogs, parser, catalog, schema);
	}

	@Test
	public void testInnerJoinParse() throws Exception {
		final String fmt = "%s" + NameUtils.SPARQL_DOT + "%s";
		final String[] colNames = {
				String.format(fmt, "foo", "StringCol"),
				String.format(fmt, "foo", "NullableStringCol"),
				String.format(fmt, "foo", "IntCol"), "NullableIntCol",
				String.format(fmt, "bar", "BarStringCol"),
				String.format(fmt, "bar", "BarNullableStringCol"),
				String.format(fmt, "bar", "BarIntCol")
		};
		final Query q = getQuery("SELECT * FROM foo inner join bar using (NullableIntCol)");
		tests.put(ElementBind.class, 7);
		tests.put(ElementFilter.class, 3);
		tests.put(ElementService.class, 1);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(7, vLst.size());
		for (final Var v : vLst) {
			Assert.assertTrue(Arrays.asList(colNames).contains(v.getName()));
		}

		final ElementService service = (ElementService) results
				.get(ElementService.class).lst.get(0);
		final Query q2 = ((ElementSubQuery) service.getElement()).getQuery();

		final List<Element> bindList = results.get(ElementBind.class).lst;
		for (final Element el : bindList) {
			final ElementBind bind = (ElementBind) el;
			Assert.assertTrue("result vars missing bind var " + bind.getVar(),
					q.getProjectVars().contains(bind.getVar()));
			final E_Function func = (E_Function) bind.getExpr();
			Assert.assertTrue(
					"service vars missing bind var " + bind.getExpr(),
					q2.getProjectVars().contains(
							((ExprVar) func.getArg(1)).asVar()));
		}
	}

	@Test
	public void testMethodParse() throws Exception {
		final Query q = getQuery("SELECT count( IntCol ) FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementService.class, 1);
		results = validate(q, tests);

		final ElementService service = (ElementService) results
				.get(ElementService.class).lst.get(0);
		final Query q2 = ((ElementSubQuery) service.getElement()).getQuery();

		assertEquals(1, q.getProjectVars().size());
		final ExprAggregator e = (ExprAggregator) q.getProject().getExpr(
				q.getProjectVars().get(0));
		final Var v2 = e.getAggregator().getExpr().asVar();
		assertTrue(v2.toString() + " missing from service call", q2
				.getProjectVars().contains(v2));
	}

	@Test
	public void testMethodWithAliasParse() throws Exception {
		final Query q = getQuery("SELECT count( IntCol ) AS bar FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementService.class, 1);
		results = validate(q, tests);

		Assert.assertEquals(1, q.getProject().size());
		Assert.assertEquals(1, q.getProjectVars().size());
		Assert.assertEquals("bar", q.getProjectVars().get(0).getVarName());

		final ElementService service = (ElementService) results
				.get(ElementService.class).lst.get(0);
		final Query q2 = ((ElementSubQuery) service.getElement()).getQuery();

		assertEquals(1, q.getProjectVars().size());
		final ExprAggregator e = (ExprAggregator) q.getProject().getExpr(
				q.getProjectVars().get(0));
		final Var v2 = e.getAggregator().getExpr().asVar();
		assertTrue(v2.toString() + " missing from service call", q2
				.getProjectVars().contains(v2));

	}

	@Test
	public void testNoColParse() throws Exception {
		final String[] colNames = {
				"StringCol", "NullableStringCol", "IntCol", "NullableIntCol"
		};

		final Query q = getQuery("SELECT * FROM foo");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementService.class, 1);
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

		final ElementService srv = (ElementService) results
				.get(ElementService.class).lst.get(0);
		final ElementSubQuery esq = (ElementSubQuery) srv.getElement();
		final List<Var> vars = esq.getQuery().getProjectVars();
		Assert.assertEquals(4, vars.size());

	}

	@Test
	public void testSpecColParse() throws Exception {
		final Query q = getQuery("SELECT StringCol FROM foo");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementService.class, 1);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

		final Element el = results.get(ElementBind.class).lst.get(0);
		final ElementBind eb = (ElementBind) el;
		Assert.assertEquals("StringCol", eb.getVar().getName());

		final ElementService srv = (ElementService) results
				.get(ElementService.class).lst.get(0);
		final ElementSubQuery esq = (ElementSubQuery) srv.getElement();
		final List<Var> vars = esq.getQuery().getProjectVars();
		// all 4 columns in foo
		Assert.assertEquals(4, vars.size());
	}

	@Test
	public void testSpecColWithAliasParse() throws Exception {
		final Query q = getQuery("SELECT StringCol AS bar FROM foo");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementService.class, 1);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("bar"), vLst.get(0));

		final ElementBind eb = (ElementBind) results.get(ElementBind.class).lst
				.get(0);
		final String name = eb.getVar().getName();
		Assert.assertEquals("bar", name);

	}

	@Test
	public void testSpecColWithEqnParse() throws Exception {
		final Query q = getQuery("SELECT StringCol FROM foo WHERE StringCol != 'baz'");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementService.class, 1);
		results = validate(q, tests);

		final ElementBind eb = (ElementBind) results.get(ElementBind.class).lst
				.get(0);
		final String name = eb.getVar().getName();
		Assert.assertEquals("StringCol", name);

		final ElementService srv = (ElementService) results
				.get(ElementService.class).lst.get(0);
		final ElementSubQuery esq = (ElementSubQuery) srv.getElement();
		final List<Var> vars = esq.getQuery().getProjectVars();
		Assert.assertEquals(4, vars.size());
		tests.clear();
		tests.put(ElementFilter.class, 1);
		results = validate(srv.getElement(), tests);

		final Expr expr = ((ElementFilter) results.get(ElementFilter.class).lst
				.get(0)).getExpr();
		Assert.assertTrue(expr instanceof E_NotEquals);
		final E_NotEquals expr2 = (E_NotEquals) expr;
		Assert.assertEquals("v_571a89fd_0385_38d9_9ef2_c37d2542d0ad",
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("baz",
				((NodeValueString) (expr2.getArg2())).asString());

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

	}

	@Test
	public void testTableAliasParse() throws Exception {
		final Query q = getQuery("SELECT StringCol FROM foo bar WHERE StringCol != 'baz'");

		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementService.class, 1);
		tests.put(ElementPathBlock.class, 4);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

		// 2 required columns
		final String[] names = {
				"StringCol", "IntCol"
		};

		for (final Element el : results.get(ElementBind.class).lst) {
			final ElementBind eb = (ElementBind) el;
			final String name = eb.getVar().getName();
			Assert.assertTrue(Arrays.asList(names).contains(name));
		}

		// should be the first one
		final Expr expr = ((ElementFilter) results.get(ElementFilter.class).lst
				.get(0)).getExpr();
		Assert.assertTrue(expr instanceof E_NotEquals);
		final E_NotEquals expr2 = (E_NotEquals) expr;
		Assert.assertEquals("v_c7b696e2_e6e6_372c_807d_51154bb155e1",
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("baz",
				((NodeValueString) (expr2.getArg2())).asString());

		verifyTable(results.get(ElementPathBlock.class),
				"v_31c94ccf_f177_3e10_a2f3_e790f85b33c5");

	}

	@Test
	public void testTwoTableJoin() throws Exception {
		final String[] columnNames = {
				"foo" + NameUtils.SPARQL_DOT + "IntCol",
				"foo" + NameUtils.SPARQL_DOT + "StringCol",
				"foo" + NameUtils.SPARQL_DOT + "NullableStringCol",
				"foo" + NameUtils.SPARQL_DOT + "NullableIntCol",
				"bar" + NameUtils.SPARQL_DOT + "BarStringCol",
				"bar" + NameUtils.SPARQL_DOT + "BarNullableStringCol",
				"bar" + NameUtils.SPARQL_DOT + "BarIntCol",
				"bar" + NameUtils.SPARQL_DOT + "NullableIntCol"
		};
		final Query q = getQuery("SELECT * FROM foo, bar WHERE foo.IntCol = bar.BarIntCol");

		tests.put(ElementBind.class, columnNames.length);
		tests.put(ElementFilter.class, 3);
		tests.put(ElementOptional.class, 4);
		tests.put(ElementService.class, 1);
		results = validate(q, tests);

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(columnNames.length, vLst.size());
		for (final Var v : vLst) {
			final ColumnName tn = ColumnName.getNameInstance("testCatalog",
					"testSchema", "table", v.getName());
			tn.setUsedSegments(NameSegments.FFTT);
			Assert.assertTrue("missing " + tn.getSPARQLName(),
					Arrays.asList(columnNames).contains(tn.getSPARQLName()));
		}

		final Expr expr = ((ElementFilter) results.get(ElementFilter.class).lst
				.get(0)).getExpr();
		Assert.assertTrue(expr instanceof E_Equals);
		final E_Equals expr2 = (E_Equals) expr;
		Assert.assertEquals("v_f87609c4_9dab_3bf7_8522_ccd46fa4e808",
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("v_1f5a248c_0097_30a6_b600_444a479c14b4",
				((ExprVar) (expr2.getArg2())).getVarName());
	}
}
