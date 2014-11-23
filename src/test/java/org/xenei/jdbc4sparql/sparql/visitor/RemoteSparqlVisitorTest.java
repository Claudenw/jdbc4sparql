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
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;

import java.io.StringReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlVisitor;
import org.xenei.jdbc4sparql.utils.ElementExtractor;

public class RemoteSparqlVisitorTest {
	private Map<String, Catalog> catalogs;
	private SparqlParser parser;
	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private SparqlVisitor sv;

	private static final String CATALOG_NAME = "testCatalog";
	private static final String SCHEMA_NAME = "testSchema";

	@Before
	public void setUp() throws Exception {
		catalogs = new HashMap<String, Catalog>();
		catalogs.put(VirtualCatalog.NAME, new VirtualCatalog());

		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("com.hp.hpl.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);
		final Model model = ModelFactory.createDefaultModel();
		final Model localModel = ModelFactory.createDefaultModel();
		final RdfCatalog catalog = new RdfCatalog.Builder()
				.setSparqlEndpoint(new URL("http://example.com/sparql"))
				.setLocalModel(localModel).setName(CATALOG_NAME).build(model);

		catalogs.put(catalog.getShortName(), catalog);

		RdfSchema schema = new RdfSchema.Builder().setCatalog(catalog)
				.setName(SCHEMA_NAME).build(model);

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
				.addQuerySegment("%1$s a <http://example.com/foo> .");
		bldr.getColumn(0).addQuerySegment(
				"%1$s <http://example.com/zero> %2$s .");
		bldr.getColumn(1).addQuerySegment(
				"%1$s <http://example.com/one> %2$s .");
		bldr.getColumn(2).addQuerySegment(
				"%1$s <http://example.com/two> %2$s .");
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
				"%1$s <http://example.com/zero> %2$s .");
		bldr.getColumn(1).addQuerySegment(
				"%1$s <http://example.com/one> %2$s .");
		bldr.getColumn(2).addQuerySegment(
				"%1$s <http://example.com/two> %2$s .");
		bldr.getColumn(3).addQuerySegment(
				"%1$s <http://example.com/three> %2$s .");
		bldr.build(model);

		parser = new SparqlParserImpl();
		sv = new SparqlVisitor(catalogs, parser, catalog, schema);

	}

	@Test
	public void testInnerJoinParse() throws Exception {
		String[] colNames = { "StringCol", "NullableStringCol", "IntCol",
				"NullableIntCol", "BarStringCol", "BarNullableStringCol",
				"BarIntCol" };
		final String query = "SELECT * FROM foo inner join bar using (NullableIntCol)";
		final Statement stmt = parserManager.parse(new StringReader(query));

		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(7, vLst.size());
		for (Var v : vLst) {
			Assert.assertTrue(Arrays.asList(colNames).contains(v.getName()));
		}

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		Assert.assertEquals(7, extractor.getExtracted().size());
		for (Element el : extractor.getExtracted()) {
			ElementBind bind = (ElementBind) el;
			Assert.assertTrue(q.getProjectVars().contains(bind.getVar()));
		}

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		// 8 columns + 1 equality
		Assert.assertEquals(9, extractor.getExtracted().size());

	}

	@Test
	public void testNoColParse() throws Exception {
		final String[] colNames = { "StringCol", "NullableStringCol", "IntCol",
				"NullableIntCol" };

		final String query = "SELECT * FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(4, vLst.size());
		for (Var v : vLst) {
			Assert.assertTrue(Arrays.asList(colNames).contains(v.getName()));
		}

		Assert.assertTrue(q.getQueryPattern() instanceof ElementGroup);
		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		Assert.assertEquals(4, extractor.getExtracted().size());
		for (Element el : extractor.getExtracted()) {
			ElementBind bind = (ElementBind) el;
			Assert.assertTrue(q.getProjectVars().contains(bind.getVar()));
		}

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		Assert.assertEquals(4, extractor.getExtracted().size());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementOptional.class));
		Assert.assertEquals(2, extractor.getExtracted().size());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementService.class));
		Assert.assertEquals(1, extractor.getExtracted().size());
		ElementService srv = (ElementService) extractor.getExtracted().get(0);
		ElementSubQuery esq = (ElementSubQuery) srv.getElement();
		List<Var> vars = esq.getQuery().getProjectVars();
		Assert.assertEquals(4, vars.size());

	}

	@Test
	public void testSpecColParse() throws Exception {
		final String query = "SELECT StringCol FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		// 2 required columns
		String[] names = { "StringCol", "IntCol" };
		Assert.assertEquals(2, extractor.getExtracted().size());
		for (Element el : extractor.getExtracted()) {
			ElementBind eb = (ElementBind) el;
			String name = eb.getVar().getName();
			Assert.assertTrue(Arrays.asList(names).contains(name));
		}

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		Assert.assertEquals(2, extractor.getExtracted().size());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementService.class));
		Assert.assertEquals(1, extractor.getExtracted().size());
		ElementService srv = (ElementService) extractor.getExtracted().get(0);
		ElementSubQuery esq = (ElementSubQuery) srv.getElement();
		List<Var> vars = esq.getQuery().getProjectVars();
		Assert.assertEquals(2, vars.size());

	}

	@Test
	public void testSpecColWithEqnParse() throws Exception {
		final String query = "SELECT StringCol FROM foo WHERE StringCol != 'baz'";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		// 2 required columns
		String[] names = { "StringCol", "IntCol" };
		Assert.assertEquals(2, extractor.getExtracted().size());
		for (Element el : extractor.getExtracted()) {
			ElementBind eb = (ElementBind) el;
			String name = eb.getVar().getName();
			Assert.assertTrue(Arrays.asList(names).contains(name));
		}

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		// 2 binds and 1 real filter
		Assert.assertEquals(3, extractor.getExtracted().size());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementService.class));
		Assert.assertEquals(1, extractor.getExtracted().size());
		ElementService srv = (ElementService) extractor.getExtracted().get(0);
		ElementSubQuery esq = (ElementSubQuery) srv.getElement();
		List<Var> vars = esq.getQuery().getProjectVars();
		Assert.assertEquals(2, vars.size());
		srv.visit(extractor.reset().setMatchType(ElementFilter.class));
		// 1 filter inside of service call
		Assert.assertEquals(1, extractor.getExtracted().size());
		Expr expr = ((ElementFilter) extractor.getExtracted().get(0)).getExpr();
		Assert.assertTrue(expr instanceof E_NotEquals);
		E_NotEquals expr2 = (E_NotEquals) expr;
		Assert.assertEquals("v_571a89fd_0385_38d9_9ef2_c37d2542d0ad",
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("baz",
				((NodeValueString) (expr2.getArg2())).asString());

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

	}

	@Test
	public void testSpecColWithAliasParse() throws Exception {
		final String query = "SELECT StringCol AS bar FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("bar"), vLst.get(0));

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		// 3 required columns
		String[] names = { "StringCol", "IntCol", "bar" };
		Assert.assertEquals(3, extractor.getExtracted().size());
		for (Element el : extractor.getExtracted()) {
			ElementBind eb = (ElementBind) el;
			String name = eb.getVar().getName();
			Assert.assertTrue(Arrays.asList(names).contains(name));
		}

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		Assert.assertEquals(3, extractor.getExtracted().size());
	}

	@Test
	public void testTableAliasParse() throws Exception {
		final String query = "SELECT StringCol FROM foo bar WHERE StringCol != 'baz'";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(1, vLst.size());
		Assert.assertEquals(Var.alloc("StringCol"), vLst.get(0));

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		// 2 required columns
		String[] names = { "StringCol", "IntCol" };
		Assert.assertEquals(2, extractor.getExtracted().size());
		for (Element el : extractor.getExtracted()) {
			ElementBind eb = (ElementBind) el;
			String name = eb.getVar().getName();
			Assert.assertTrue(Arrays.asList(names).contains(name));
		}

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		// 2 binds and 1 real filter
		Assert.assertEquals(3, extractor.getExtracted().size());

		// should be the first one
		Expr expr = ((ElementFilter) extractor.getExtracted().get(0)).getExpr();
		Assert.assertTrue(expr instanceof E_NotEquals);
		E_NotEquals expr2 = (E_NotEquals) expr;
		Assert.assertEquals("v_c7b696e2_e6e6_372c_807d_51154bb155e1",
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("baz",
				((NodeValueString) (expr2.getArg2())).asString());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementPathBlock.class));
		Assert.assertEquals(3, extractor.getExtracted().size());
		ElementPathBlock epb = (ElementPathBlock) extractor.getExtracted().get(
				0);
		Iterator<TriplePath> iter = epb.patternElts();
		while (iter.hasNext()) {
			TriplePath t = iter.next();
			Assert.assertTrue(t.getSubject().isVariable());
			Assert.assertEquals("v_31c94ccf_f177_3e10_a2f3_e790f85b33c5", t
					.getSubject().getName());
		}
	}

	@Test
	public void testTwoTableJoin() throws Exception {
		final String[] columnNames = { "foo" + NameUtils.SPARQL_DOT + "IntCol",
				"foo" + NameUtils.SPARQL_DOT + "StringCol",
				"foo" + NameUtils.SPARQL_DOT + "NullableStringCol",
				"foo" + NameUtils.SPARQL_DOT + "NullableIntCol",
				"bar" + NameUtils.SPARQL_DOT + "BarStringCol",
				"bar" + NameUtils.SPARQL_DOT + "BarNullableStringCol",
				"bar" + NameUtils.SPARQL_DOT + "BarIntCol",
				"bar" + NameUtils.SPARQL_DOT + "NullableIntCol" };
		final String query = "SELECT * FROM foo, bar WHERE foo.IntCol = bar.BarIntCol";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		//
		final List<Var> vLst = q.getProjectVars();
		Assert.assertEquals(columnNames.length, vLst.size());
		for (Var v : vLst) {
			ColumnName tn = ColumnName.getNameInstance("testCatalog",
					"testSchema", "table", v.getName());
			tn.setUsedSegments(new NameSegments(false, false, true, true));
			Assert.assertTrue("missing " + tn.getSPARQLName(),
					Arrays.asList(columnNames).contains(tn.getSPARQLName()));
		}

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		Assert.assertEquals(columnNames.length, extractor.getExtracted().size());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		Assert.assertEquals(columnNames.length + 1, extractor.getExtracted()
				.size());
		Expr expr = ((ElementFilter) extractor.getExtracted().get(0)).getExpr();
		Assert.assertTrue(expr instanceof E_Equals);
		E_Equals expr2 = (E_Equals) expr;
		Assert.assertEquals("v_f87609c4_9dab_3bf7_8522_ccd46fa4e808",
				((ExprVar) (expr2.getArg1())).getVarName());
		Assert.assertEquals("v_1f5a248c_0097_30a6_b600_444a479c14b4",
				((ExprVar) (expr2.getArg2())).getVarName());
	}

	@Test
	public void testMethodParse() throws Exception {
		final String query = "SELECT count( IntCol ) FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		final VarExprList vLst = q.getProject();
		Assert.assertEquals(1, vLst.getExprs().size());

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		Assert.assertEquals(1, extractor.getExtracted().size());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		Assert.assertEquals(2, extractor.getExtracted().size());
	}

	@Test
	public void testMethodWithAliasParse() throws Exception {
		final String query = "SELECT count( IntCol ) AS bar FROM foo";
		final Statement stmt = parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		final Query q = sv.getBuilder().build();

		Assert.assertEquals(1, q.getProject().size());
		Assert.assertEquals(1, q.getProjectVars().size());
		Assert.assertEquals("bar", q.getProjectVars().get(0).getVarName());

		ElementExtractor extractor = new ElementExtractor(ElementBind.class);
		q.getQueryPattern().visit(extractor);
		Assert.assertEquals(1, extractor.getExtracted().size());

		q.getQueryPattern().visit(
				extractor.reset().setMatchType(ElementFilter.class));
		Assert.assertEquals(2, extractor.getExtracted().size());
	}
}
