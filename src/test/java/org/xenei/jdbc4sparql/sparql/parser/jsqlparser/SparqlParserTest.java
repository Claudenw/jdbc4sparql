package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLDataException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.LoggingConfig;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfo;
import org.xenei.jdbc4sparql.utils.ElementExtractor;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;

/**
 * Class that validates the parser correctly parses the SQL into SPARQL
 *
 */
public class SparqlParserTest {

	private SparqlParser parser;
	private Map<String, Catalog> catalogs;
	private RdfCatalog catalog;
	private RdfSchema schema;
	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private Query query;
	private Map<Class<? extends Element>, Integer> tests;
	private Map<Class<? extends Element>, Wrapper> results;
	private TableName fooTableName;
	private TableName barTableName;
	private List<String> vars;

	private static String RQD = "IntCol";
	private static String OPT = "NullableStringCol";
	private static String RQD_TEST = "StringCol";
	private static String OPT_TEST = "NullableIntCol";

	@Before
	public void setup() {
		catalogs = new HashMap<String, Catalog>();
		catalogs.put(VirtualCatalog.NAME, new VirtualCatalog());
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("com.hp.hpl.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);

		final Model model = ModelFactory.createDefaultModel();
		final Model localModel = ModelFactory.createDefaultModel();
		catalog = new RdfCatalog.Builder().setLocalModel(localModel)
				.setName("testCatalog").build(model);
		catalogs.put(catalog.getShortName(), catalog);

		schema = new RdfSchema.Builder().setCatalog(catalog)
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
		fooTableName = bldr.getName();
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
		barTableName = bldr.getName();
		bldr.build(model);
		parser = new SparqlParserImpl();
		tests = new HashMap<Class<? extends Element>, Integer>();
	}

	@Test
	public void testGetSupportedNumericFunctions() {
		assertNotNull(parser.getSupportedNumericFunctions());
	}

	@Test
	public void testGetSupportedStringFunctions() {
		assertNotNull(parser.getSupportedStringFunctions());
	}

	@Test
	public void testGetSupportedSystemFunctions() {
		assertNotNull(parser.getSupportedSystemFunctions());
	}

	// /**
	// * Parse the SQL string and then deparse it back into SQL to provide the
	// SQL
	// * string native to the parser. This is used in support of nativeSQL() in
	// * the Driver.
	// *
	// * @param sqlQuery
	// * the original SQL string
	// * @return the native SQL string
	// * @throws SQLException
	// * on error.
	// */
	// String nativeSQL(String sqlQuery) throws SQLException;

	private Query getQuery(final String sql) throws SQLDataException,
			JSQLParserException {
		final Statement stmt = parserManager.parse(new StringReader(sql));
		final SparqlVisitor sparqlVisitor = new SparqlVisitor(catalogs, parser,
				catalog, schema);
		stmt.accept(sparqlVisitor);
		return sparqlVisitor.getBuilder().build();
	}

	private class Wrapper {
		List<Element> lst;
		Integer i;
	}

	private Map<Class<? extends Element>, Wrapper> validate(final Query query,
			final Map<Class<? extends Element>, Integer> tests)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		final Map<Class<? extends Element>, Wrapper> retval = new IdentityHashMap<Class<? extends Element>, Wrapper>();
		final ElementExtractor extractor = new ElementExtractor();
		for (final Map.Entry<Class<? extends Element>, Integer> test : tests
				.entrySet()) {
			query.getQueryPattern().visit(
					extractor.setMatchType(test.getKey()).reset());
			assertEquals("Wrong number of " + test.getKey(), test.getValue(),
					Integer.valueOf(extractor.getExtracted().size()));
			final Wrapper wrapper = new Wrapper();
			wrapper.lst = extractor.getExtracted();
			wrapper.i = extractor.getExtracted().size();
			retval.put(test.getKey(), wrapper);
		}
		return retval;
	}

	private void verifyVars(final List<Var> vars, final String[] names) {
		assertEquals(names.length, vars.size());
		for (int i = 0; i < names.length; i++) {
			assertEquals(names[i], vars.get(i).getName());
		}
	}

	private List<String> verifyTable(final Wrapper w, final String name) {
		final List<String> retval = new ArrayList<String>();
		for (final Element el : w.lst) {
			final ElementPathBlock epb = (ElementPathBlock) el;
			final Iterator<TriplePath> iter = epb.patternElts();
			while (iter.hasNext()) {
				final TriplePath t = iter.next();
				Assert.assertTrue(t.getSubject().isVariable());
				Assert.assertEquals(name, t.getSubject().getName());
				final Node n = t.getObject();
				if (n.isVariable()) {
					retval.add(t.getObject().getName());
				}
			}
		}
		return retval;
	}

	private void verifyBinds(final Wrapper w, final List<String> cols) {
		for (final Element el : w.lst) {
			final ElementBind bind = (ElementBind) el;
			final Expr expr = bind.getExpr();
			assertTrue("Not an E_Function", expr instanceof E_Function);
			assertEquals(ForceTypeF.IRI, ((E_Function) expr).getFunctionIRI());
			final String name = ((E_Function) expr).getArg(1).getVarName();
			assertTrue("Missing " + name, cols.contains(name));
		}
	}

	@Test
	public void testCatalogFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedSystemFunctions();
		assertTrue(funcs.contains("CATALOG"));

		query = getQuery("SELECT CATALOG() FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT CATALOG() as arg FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testVersionFunctionSelect() throws Exception {
		final List<String> funcs = parser.getSupportedSystemFunctions();
		assertTrue(funcs.contains("VERSION"));

		query = getQuery("SELECT VERSION() FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT VERSION() as arg FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());

	}

	@Test
	public void testCatalogFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedSystemFunctions();
		assertTrue(funcs.contains("CATALOG"));

		query = getQuery("SELECT * FROM foo WHERE StringCol=CATALOG()");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testVersionFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedSystemFunctions();
		assertTrue(funcs.contains("VERSION"));

		query = getQuery("SELECT * FROM foo WHERE StringCol=VERSION()");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
	}

	@Test
	public void testLengthFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("LENGTH"));

		query = getQuery("SELECT LENGTH(StringCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT LENGTH(StringCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testSubstringFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("SUBSTRING"));

		query = getQuery("SELECT SUBSTRING(StringCol,1,3) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT SUBSTRING(StringCol,1,3)  as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testUcaseFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("UCASE"));

		query = getQuery("SELECT UCASE(StringCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT UCASE(StringCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT UPPER(StringCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT UPPER(StringCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testLcaseFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("LCASE"));

		query = getQuery("SELECT LCASE(StringCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT LCASE(StringCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT LOWER(StringCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT LOWER(StringCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testReplaceFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("REPLACE"));

		query = getQuery("SELECT REPLACE(StringCol, 'a', 'b') FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT REPLACE(StringCol, 'a', 'b') as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());

	}

	@Test
	public void testLengthFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("LENGTH"));

		query = getQuery("SELECT * FROM foo WHERE LENGTH(StringCol)=5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testSubstringFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("SUBSTRING"));

		query = getQuery("SELECT * FROM foo WHERE SUBSTRING(StringCol,1,3)='bcd'");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testUcaseFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("UCASE"));

		query = getQuery("SELECT * FROM foo where UCASE(StringCol)='ABC' ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		Wrapper w = results.get(ElementFilter.class);
		ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

		query = getQuery("SELECT * FROM foo WHERE UPPER(StringCol)='ABC'");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		w = results.get(ElementFilter.class);
		filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testLcaseFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("LCASE"));

		query = getQuery("SELECT * FROM foo WHERE LCASE(StringCol)='abc'");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		Wrapper w = results.get(ElementFilter.class);
		ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

		query = getQuery("SELECT * FROM foo WHERE LOWER(StringCol)='abc'");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		w = results.get(ElementFilter.class);
		filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testReplaceFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedStringFunctions();
		assertTrue(funcs.contains("REPLACE"));

		query = getQuery("SELECT * FROM foo WHERE REPLACE(StringCol, 'a', 'b')='bbc'");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

	}

	@Test
	public void testMaxFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("MAX"));

		query = getQuery("SELECT MAX(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT MAX(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testMinFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("MIN"));

		query = getQuery("SELECT MIN(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT MIN(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testCountFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("COUNT"));

		query = getQuery("SELECT COUNT(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT COUNT(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT COUNT(*) FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT COUNT(*) as arg FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());

	}

	@Test
	public void testSumFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("SUM"));

		query = getQuery("SELECT SUM(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT SUM(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testAbsFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("ABS"));

		query = getQuery("SELECT ABS(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT ABS(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testRoundFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("ROUND"));

		query = getQuery("SELECT ROUND(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT ROUND(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testCeilFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("CEIL"));

		query = getQuery("SELECT CEIL(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT CEIL(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testFloorFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("FLOOR"));

		query = getQuery("SELECT FLOOR(IntCol) FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT FLOOR(IntCol) as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	@Test
	public void testRandFunctionSelect() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("RAND"));

		query = getQuery("SELECT RAND() FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT RAND() as arg FROM foo");
		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals("arg", query.getProjectVars().get(0).getName());

	}

	@Test
	public void testMaxFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("MAX"));

		query = getQuery("SELECT * FROM foo WHERE MAX(IntCol)=5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

	}

	@Test
	public void testMinFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("MIN"));

		query = getQuery("SELECT * FROM foo WHERE MIN(IntCol)=2");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testCountFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("COUNT"));

		query = getQuery("SELECT * FROM foo WHERE COUNT(IntCol) = 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		Wrapper w = results.get(ElementFilter.class);
		ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

		query = getQuery("SELECT * FROM foo WHERE COUNT(*) = 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		w = results.get(ElementFilter.class);
		filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testSumFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("SUM"));

		query = getQuery("SELECT * FROM foo WHERE SUM(IntCol) = 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testAbsFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("ABS"));

		query = getQuery("SELECT * FROM foo WHERE ABS(IntCol) = 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testRoundFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("ROUND"));

		query = getQuery("SELECT * FROM foo WHERE  ROUND(IntCol) =5 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testCeilFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("CEIL"));

		query = getQuery("SELECT * FROM foo WHERE CEIL(IntCol) =5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testFloorFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("FLOOR"));

		query = getQuery("SELECT * FROM foo WHERE FLOOR(IntCol) = 3");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testRandFunctionWhere() throws Exception {

		final List<String> funcs = parser.getSupportedNumericFunctions();
		assertTrue(funcs.contains("RAND"));

		query = getQuery("SELECT * FROM foo WHERE RAND() < 0.5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

	}

	@Test
	public void testAddWhere() throws Exception {

		query = getQuery("SELECT * FROM foo WHERE IntCol + 3 > 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		Wrapper w = results.get(ElementFilter.class);
		ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

		query = getQuery("SELECT * FROM foo WHERE IntCol + 3 > 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		w = results.get(ElementFilter.class);
		filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

	}

	// try {
	// query = getQuery("SELECT * FROM foo WHERE IntCol > ALL( 2, 3, 5 )");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }
	@Test
	public void testAndWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol > 5 AND IntCol < 7");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	// try {
	// query = getQuery("SELECT * FROM foo WHERE IntCol > ANY( 2, 3, 5 )");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }
	@Test
	public void testBetweenWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol BETWEEN 5 AND 7");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

	}

	// try {
	// query = getQuery("SELECT * FROM foo WHERE 5 & 7");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }
	//
	// try {
	// query = getQuery("SELECT * FROM foo WHERE 5 | 7");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }
	// try {
	// query = getQuery("SELECT * FROM foo WHERE 5 ^ 7");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// try {
	// query =
	// getQuery("SELECT * FROM foo WHERE CASE WHEN IntCol < 5 THEN StringCol != 'foo' WHEN IntCol > 7 THEN StringCol != 'bar' ELSE FALSE END");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	@Test
	public void testEqualsWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol = 7");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testBinaryOrWhere() throws Exception {
		try {
			query = getQuery("SELECT * FROM foo WHERE StringCol = TRUE || FALSE");
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
			// do nothing
		}
	}

	// query =
	// getQuery("SELECT * FROM foo WHERE DATEVALUE( StringCol ) > IntCol");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	@Test
	public void testDivisionWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol / 5 > 2.3");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testDoubleWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE  2.3 > IntCol");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testLongWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE  3 = IntCol");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testExistsWhere() throws Exception {
		try {
			query = getQuery("SELECT * FROM foo WHERE EXISTS ( SELECT * FROM bar )");
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
			// do nothing
		}

	}

	@Test
	public void testGreaterThanWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol > 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testGreaterThanEqualWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol >= 5");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testInWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol IN ( 2,4,6)");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	//
	// @Override
	// public void visit(final InverseExpression inverseExpression) {
	// throw new UnsupportedOperationException(
	// "inverse expressions are not supported");
	// }

	@Test
	public void testIsNullWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE NullableStringCol IS NULL");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		Wrapper w = results.get(ElementFilter.class);
		ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

		query = getQuery("SELECT * FROM foo WHERE NullableStringCol IS NOT NULL ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		w = results.get(ElementFilter.class);
		filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	// @Override
	// public void visit(final JdbcParameter jdbcParameter) {
	// throw new UnsupportedOperationException(
	// "JDBC Parameters are not supported");
	// }

	@Test
	public void testLikeWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE StringCol LIKE 'A_b%c' ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	// query = getQuery("SELECT * FROM foo WHERE (LONG)5 > IntCol ");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	// try {
	// query = getQuery("SELECT * FROM foo WHERE StringCol MATCHES 'A?B*C'");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	@Test
	public void testLessThanWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol < 5 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testLessThanEqualWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE  IntCol <= 5 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testMultiplicationWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE  IntCol * 5 > 20 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testNotEqualsWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE  IntCol <> 5 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		Wrapper w = results.get(ElementFilter.class);
		ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);

		query = getQuery("SELECT * FROM foo WHERE IntCol != 5 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		w = results.get(ElementFilter.class);
		filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	// try {
	// query = getQuery("SELECT * FROM foo WHERE NULL");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	@Test
	public void testLogicalOrWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol=5 OR IntCol=6 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testParenthesisWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE ( IntCol > 5 ) ");

		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	@Test
	public void testSubQueryWhere() throws Exception {

		try {
			query = getQuery("SELECT * FROM foo WHERE IntCol = (SELECT BarIntCol FROM bar)");
			fail("Should have thrown UnsupportedOperationException");
		} catch (final UnsupportedOperationException expected) {
			// do nothing
		}
	}

	@Test
	public void testSubtractWhere() throws Exception {
		query = getQuery("SELECT * FROM foo WHERE IntCol-5 > 5 ");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();

		final Wrapper w = results.get(ElementFilter.class);
		final ElementFilter filter = (ElementFilter) w.lst.get(1);
		assertFalse(filter.getExpr() instanceof ExprInfo);
	}

	// query =
	// getQuery("SELECT * FROM foo WHERE TimeStamp( StringCol ) > NOW()");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	//
	// query = getQuery("SELECT * FROM foo WHERE Time( StringCol ) > NOW()");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	// // when
	// try {
	// query =
	// getQuery("SELECT * FROM foo WHERE CASE WHEN IntCol < 5 THEN StringCol != 'foo' WHEN IntCol > 7 THEN StringCol != 'bar' ELSE FALSE END");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	@Test
	public void testAddSelect() throws Exception {

		query = getQuery("SELECT IntCol + 3 FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT IntCol + 3 as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	// query = getQuery("SELECT IntCol > 5  FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol___5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol > 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// try {
	// query = getQuery("SELECT * FROM foo WHERE IntCol > ALL( 2, 3, 5 )");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }

	// query = getQuery("SELECT  FROM foo WHERE IntCol > 5 AND IntCol < 7");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	// try {
	// query = getQuery("SELECT * FROM foo WHERE IntCol > ANY( 2, 3, 5 )");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }

	// query = getQuery("SELECT * FROM foo WHERE IntCol BETWEEN 5 AND 7");
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	// try {
	// query = getQuery("SELECT * FROM foo WHERE 5 & 7");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }
	//
	// try {
	// query = getQuery("SELECT * FROM foo WHERE 5 | 7");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothingl
	// }
	// try {
	// query = getQuery("SELECT * FROM foo WHERE 5 ^ 7");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// try {
	// query =
	// getQuery("SELECT * FROM foo WHERE CASE WHEN IntCol < 5 THEN StringCol != 'foo' WHEN IntCol > 7 THEN StringCol != 'bar' ELSE FALSE END");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// query = getQuery("SELECT IntCol = 7 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol__7", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol = 7 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());
	//

	// try {
	// query = getQuery("SELECT * FROM foo WHERE StringCol = TRUE || FALSE");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// query =
	// getQuery("SELECT * FROM foo WHERE DATEVALUE( StringCol ) > IntCol");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	@Test
	public void testDivisionSelect() throws Exception {

		query = getQuery("SELECT IntCol / 5 FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT IntCol / 5 as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	// query = getQuery("SELECT IntCol > 2.3 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol___2_3", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol > 2.3 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// query = getQuery("SELECT IntCol = 3 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol___3", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol = 3 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// try {
	// query = getQuery("SELECT * FROM foo WHERE EXISTS ( SELECT * FROM bar )");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// query = getQuery("SELECT IntCol > 5 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol___5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol > 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol >= 5 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol____5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol >= 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// query = getQuery("SELECT IntCol IN (2,4,6) FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol_IN________",
	// query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol IN (2,4,6) as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	//
	// @Override
	// public void visit(final InverseExpression inverseExpression) {
	// throw new UnsupportedOperationException(
	// "inverse expressions are not supported");
	// }

	// query = getQuery("SELECT NullableStringCol IS NULL FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("NullableStringCol_IS_NULL",
	// query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT NullableStringCol IS NULL as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT NullableStringCol IS NOT NULL FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("NullableStringCol_IS_NOT_NULL",
	// query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT NullableStringCol IS NOT NULL as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// @Override
	// public void visit(final JdbcParameter jdbcParameter) {
	// throw new UnsupportedOperationException(
	// "JDBC Parameters are not supported");
	// }

	// query = getQuery("SELECT StringCol LIKE 'A_b%c' FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("StringCol_LIKE __A_b_c_",
	// query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT StringCol LIKE 'A_b%c' as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());
	//

	// query = getQuery("SELECT * FROM foo WHERE (LONG)5 > IntCol ");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	// try {
	// query = getQuery("SELECT * FROM foo WHERE StringCol MATCHES 'A?B*C'");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// query = getQuery("SELECT IntCol < 5 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol___5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol < 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol <= 5 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol____5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol <= 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	@Test
	public void testMultiplicationSelect() throws Exception {
		query = getQuery("SELECT IntCol * 5 FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT IntCol * 5 as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	// query = getQuery("SELECT IntCol <> 5 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol____5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol <> 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// query = getQuery("SELECT IntCol != 5 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol____5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol != 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// try {
	// query = getQuery("SELECT * FROM foo WHERE NULL");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// query = getQuery("SELECT TRUE OR FALSE FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("TRUE_OR_FALSE", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT TRUE OR FALSE as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// query = getQuery("SELECT ( IntCol > 5 ) FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("__IntCol___5__", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT ( IntCol > 5 ) as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// query = getQuery("SELECT IntCol != 5 FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("IntCol____5", query.getProjectVars().get(0).getName());
	//
	// query = getQuery("SELECT IntCol != 5 as arg FROM foo");
	// tests.put(ElementBind.class, 1);
	// tests.put(ElementFilter.class, 1);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	// assertEquals(1, query.getProjectVars().size());
	// assertEquals("arg", query.getProjectVars().get(0).getName());

	// try {
	// query =
	// getQuery("SELECT * FROM foo WHERE IntCol = (SELECT BarIntCol FROM bar)");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	@Test
	public void testSubtractionSelect() throws Exception {
		query = getQuery("SELECT IntCol-5 FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("var0", query.getProjectVars().get(0).getName());

		query = getQuery("SELECT IntCol-5 as arg FROM foo");
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
		assertEquals(1, query.getProjectVars().size());
		assertEquals("arg", query.getProjectVars().get(0).getName());
	}

	// query =
	// getQuery("SELECT * FROM foo WHERE TimeStamp( StringCol ) > NOW()");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();
	//
	// query = getQuery("SELECT * FROM foo WHERE Time( StringCol ) > NOW()");
	//
	// tests.put(ElementBind.class, 4);
	// tests.put(ElementFilter.class, 2);
	// tests.put(ElementOptional.class, 2);
	// results = validate(query, tests);
	// tests.clear();

	// try {
	// query =
	// getQuery("SELECT * FROM foo WHERE IntCol = (SELECT BarIntCol FROM bar)");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	// // when
	// try {
	// query =
	// getQuery("SELECT * FROM foo WHERE CASE WHEN IntCol < 5 THEN StringCol != 'foo' WHEN IntCol > 7 THEN StringCol != 'bar' ELSE FALSE END");
	// fail("Should have thrown UnsupportedOperationException");
	// } catch (UnsupportedOperationException expected) {
	// // do nothing
	// }

	@Test
	public void testMultipleAnonymousFunctionSelect() throws Exception {

		final List<String> colNames = new ArrayList<String>();
		colNames.add("var0");
		colNames.add("var1");

		query = getQuery("SELECT Catalog(), Version() FROM foo");

		final List<Var> vLst = query.getProjectVars();
		assertEquals(2, vLst.size());
		for (final Var v : vLst) {
			assertTrue(v.getName() + " missing", colNames.contains(v.getName()));
			colNames.remove(v.getName());
		}

		tests.put(ElementBind.class, 0);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		tests.clear();
	}

	@Test
	public void testSimpleSelectAll() throws Exception {
		final String rqd_tst = String.format("%s='FooString'", RQD_TEST);
		final String opt_tst = String.format("%s='FooString'", OPT_TEST);

		final String[] colNames = {
				"StringCol", "NullableStringCol", "IntCol", "NullableIntCol"
		};

		query = getQuery("SELECT * FROM foo");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), colNames);

		for (final Element el : results.get(ElementBind.class).lst) {
			final ElementBind bind = (ElementBind) el;
			assertTrue(query.getProjectVars().contains(bind.getVar()));
		}
		tests.clear();
		query = getQuery(String.format("SELECT * FROM foo WHERE %s", rqd_tst));
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), colNames);
		tests.clear();

		query = getQuery(String.format("SELECT * FROM foo WHERE %s", opt_tst));
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), colNames);
		tests.clear();

	}

	@Test
	public void testSimpleSelectAllTableAlias() throws Exception {
		final String rqd_tst = String.format("%s='FooString'", RQD_TEST);
		final String opt_tst = String.format("%s='FooString'", OPT_TEST);

		final String[] colNames = {
				"StringCol", "NullableStringCol", "IntCol", "NullableIntCol"
		};

		query = getQuery("SELECT * FROM foo AS bar");
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), colNames);

		for (final Element el : results.get(ElementBind.class).lst) {
			final ElementBind bind = (ElementBind) el;
			assertTrue(query.getProjectVars().contains(bind.getVar()));
		}
		tests.clear();
		query = getQuery(String.format("SELECT * FROM foo AS bar WHERE %s",
				rqd_tst));
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), colNames);
		tests.clear();

		query = getQuery(String.format("SELECT * FROM foo AS bar WHERE %s",
				opt_tst));
		tests.put(ElementBind.class, 4);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), colNames);
		tests.clear();
	}

	@Test
	public void testSimpleSelectRequired() throws Exception {
		final String rqd_tst = String.format("%s='FooString'", RQD_TEST);
		final String opt_tst = String.format("%s='FooString'", OPT_TEST);

		query = getQuery(String.format("SELECT %s FROM foo", RQD));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			RQD
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s AS arg FROM foo", RQD));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo WHERE %s", RQD,
				rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			RQD
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s as arg FROM foo WHERE %s",
				RQD, rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo WHERE %s", RQD,
				opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			RQD
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s AS arg FROM foo WHERE %s",
				RQD, opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

	}

	@Test
	public void testSimpleSelectRequiredTableAlias() throws Exception {
		final String rqd_tst = String.format("%s='FooString'", RQD_TEST);
		final String opt_tst = String.format("%s='FooString'", OPT_TEST);

		query = getQuery(String.format("SELECT %s FROM foo AS bar", RQD));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			RQD
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s AS arg FROM foo AS bar", RQD));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo AS bar WHERE %s",
				RQD, rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			RQD
		});
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s as arg FROM foo AS bar WHERE %s", RQD, rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo AS bar WHERE %s",
				RQD, opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			RQD
		});
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s AS arg FROM foo AS bar WHERE %s", RQD, opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

	}

	@Test
	public void testSimpleSelectOptional() throws Exception {
		final String rqd_tst = String.format("%s='FooString'", RQD_TEST);
		final String opt_tst = String.format("%s='FooString'", OPT_TEST);

		query = getQuery(String.format("SELECT %s FROM foo", OPT));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			OPT
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s AS arg FROM foo", OPT));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo WHERE %s", OPT,
				rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			OPT
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s AS arg FROM foo WHERE %s",
				OPT, rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo WHERE %s", OPT,
				opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			OPT
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s AS arg FROM foo WHERE %s",
				OPT, opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				fooTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

	}

	@Test
	public void testInnerJoin_Using() throws Exception {
		// NullableIntCol is promoted to required as it is what we are joining
		// on.
		/* star select */
		query = getQuery("SELECT * FROM foo INNER JOIN bar using (NullableIntCol)");
		// 4 from each table
		tests.put(ElementBind.class, 8);
		// 1 from each table
		tests.put(ElementFilter.class, 2);
		// 1 from each table
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 10);
		results = validate(query, tests);

		// one for each column in each table minus the joined column name.
		assertEquals(7, query.getProjectVars().size());
		tests.clear();

		/* foo star select */
		query = getQuery("SELECT foo.* FROM foo INNER JOIN bar using (NullableIntCol)");
		// 4 from foo + 1 bar nullableIntCol
		tests.put(ElementBind.class, 5);
		// 1 from each table
		tests.put(ElementFilter.class, 2);
		// 1 from each table
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();

		/* OPT COLUMN */
		query = getQuery(String
				.format("SELECT %s FROM foo INNER JOIN bar using (NullableIntCol)",
						OPT));
		// 2 from foo + 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table
		tests.put(ElementFilter.class, 2);
		// 1 from each table
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		/* RQD COLUMN */
		query = getQuery(String
				.format("SELECT %s FROM foo INNER JOIN bar using (NullableIntCol)",
						RQD));
		// 2 from foo + 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table
		tests.put(ElementFilter.class, 2);
		// 1 from each table
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

	}

	@Test
	public void testInnerJoin_Star_On() throws Exception {
		query = getQuery(String.format(
				"SELECT * FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s",
				RQD_TEST, RQD_TEST));
		// 4 from each table + 2 on join
		tests.put(ElementBind.class, 10);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT * FROM foo INNER JOIN bar ON foo.%s=bar.%s", RQD_TEST,
				OPT_TEST));
		// 4 from each table + 2 on join
		tests.put(ElementBind.class, 10);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT * FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s",
				OPT_TEST, RQD_TEST));
		// 4 from each table + 2 on join
		tests.put(ElementBind.class, 10);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT * FROM foo INNER JOIN bar ON foo.%s=bar.%s", OPT_TEST,
				OPT_TEST));
		// 4 from each table + 2 on join
		tests.put(ElementBind.class, 10);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

	}

	@Test
	public void testInnerJoin_FooStar_On() throws Exception {

		/* foo star select */
		query = getQuery(String.format(
				"SELECT foo.* FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s",
				RQD_TEST, RQD_TEST));
		// 4 from foo + 2 on join
		tests.put(ElementBind.class, 6);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT foo.* FROM foo INNER JOIN bar ON foo.%s=bar.%s",
				RQD_TEST, OPT_TEST));
		// 4 from foo + 2 on join
		tests.put(ElementBind.class, 6);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT foo.* FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s",
				OPT_TEST, RQD_TEST));
		// 4 from foo + 2 on join
		tests.put(ElementBind.class, 6);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT foo.* FROM foo INNER JOIN bar ON foo.%s=bar.%s",
				OPT_TEST, OPT_TEST));
		// 4 from foo + 2 on join
		tests.put(ElementBind.class, 6);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();
	}

	@Test
	public void testInnerJoin_Opt() throws Exception {

		/* OPT COLUMN */
		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s", OPT,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.%s", OPT,
				RQD_TEST, OPT_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);

		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s", OPT,
				OPT_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.%s", OPT,
				OPT_TEST, OPT_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();
	}

	@Test
	public void testInnerJoin_Rqd() throws Exception {

		/* RQD COLUMN */
		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo INNER JOIN bar ON foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

	}

	@Test
	public void testSimpleJoin_Star() throws Exception {

		/* star select */
		query = getQuery(String.format(
				"SELECT * FROM foo, bar WHERE foo.%s=bar.Bar%s", RQD_TEST,
				RQD_TEST));
		// 4 from each table
		tests.put(ElementBind.class, 8);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT * FROM foo, bar WHERE foo.%s=bar.%s", RQD_TEST,
				OPT_TEST));
		// 4 from each table
		tests.put(ElementBind.class, 8);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT * FROM foo, bar WHERE foo.%s=bar.Bar%s", OPT_TEST,
				RQD_TEST));
		// 4 from each table
		tests.put(ElementBind.class, 8);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT * FROM foo, bar WHERE foo.%s=bar.%s", OPT_TEST,
				OPT_TEST));
		// 4 from each table
		tests.put(ElementBind.class, 8);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(8, query.getProjectVars().size());
		tests.clear();

	}

	@Test
	public void testSimpleJoin_FooStar() throws Exception {

		/* foo star select */
		query = getQuery(String.format(
				"SELECT foo.* FROM foo, bar WHERE foo.%s=bar.Bar%s", RQD_TEST,
				RQD_TEST));
		// 4 from foo + select from bar
		tests.put(ElementBind.class, 5);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT foo.* FROM foo, bar WHERE foo.%s=bar.%s", RQD_TEST,
				OPT_TEST));
		// 4 from foo + select from bar
		tests.put(ElementBind.class, 5);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT foo.* FROM foo, bar WHERE foo.%s=bar.Bar%s", OPT_TEST,
				RQD_TEST));
		// 4 from foo + select from bar
		tests.put(ElementBind.class, 5);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT foo.* FROM foo, bar WHERE foo.%s=bar.%s", OPT_TEST,
				OPT_TEST));
		// 4 from foo + select from bar
		tests.put(ElementBind.class, 5);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(4, query.getProjectVars().size());
		tests.clear();
	}

	@Test
	public void testSimpleJoin_Opt() throws Exception {

		/* OPT COLUMN */
		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.Bar%s", OPT,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.%s", OPT, RQD_TEST,
				OPT_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);

		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.Bar%s", OPT,
				OPT_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.%s", OPT, OPT_TEST,
				OPT_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();
	}

	@Test
	public void testSimpleJoin_Rqd() throws Exception {

		/* RQD COLUMN */
		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s FROM foo, bar WHERE foo.%s=bar.Bar%s", RQD,
				RQD_TEST, RQD_TEST));
		// 2 from foo, 1 from bar
		tests.put(ElementBind.class, 3);
		// 1 from each table + join filter
		tests.put(ElementFilter.class, 3);
		// 2 from each table
		tests.put(ElementOptional.class, 4);
		tests.put(ElementPathBlock.class, 10);
		validate(query, tests);
		assertEquals(1, query.getProjectVars().size());
		tests.clear();

	}

	@Test
	public void testSimpleSelectOptionalTableAlias() throws Exception {
		final String rqd_tst = String.format("%s='FooString'", RQD_TEST);
		final String opt_tst = String.format("%s='FooString'", OPT_TEST);

		query = getQuery(String.format("SELECT %s FROM foo AS bar", OPT));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			OPT
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s AS arg FROM foo AS bar", OPT));
		tests.put(ElementBind.class, 1);
		tests.put(ElementFilter.class, 1);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo AS bar WHERE %s",
				OPT, rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			OPT
		});
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s AS arg FROM foo AS bar WHERE %s", OPT, rqd_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

		query = getQuery(String.format("SELECT %s FROM foo AS bar WHERE %s",
				OPT, opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			OPT
		});
		tests.clear();

		query = getQuery(String.format(
				"SELECT %s AS arg FROM foo AS bar WHERE %s", OPT, opt_tst));
		// one for select one for filter
		tests.put(ElementBind.class, 2);
		tests.put(ElementFilter.class, 2);
		tests.put(ElementOptional.class, 2);
		tests.put(ElementPathBlock.class, 5);
		results = validate(query, tests);
		vars = verifyTable(results.get(ElementPathBlock.class),
				barTableName.getGUID());
		verifyBinds(results.get(ElementBind.class), vars);
		verifyVars(query.getProjectVars(), new String[] {
			"arg"
		});
		tests.clear();

	}

	// @Test
	// public void testInnerJoin() throws Exception {
	// final String fmt = "%s" + NameUtils.SPARQL_DOT + "%s";
	// final String[] colNames = {
	// String.format(fmt, "foo", "StringCol"),
	// String.format(fmt, "foo", "NullableStringCol"),
	// String.format(fmt, "foo", "IntCol"), "NullableIntCol",
	// String.format(fmt, "bar", "BarStringCol"),
	// String.format(fmt, "bar", "BarNullableStringCol"),
	// String.format(fmt, "bar", "BarIntCol")
	// };
	// final Query q = getQuery(
	// "SELECT * FROM foo INNER JOIN bar USING (NullableIntCol)" );
	//
	// final List<Var> vLst = q.getProjectVars();
	// assertEquals(7, vLst.size());
	// for (final Var v : vLst) {
	// assertTrue(Arrays.asList(colNames).contains(v.getName()));
	// }
	//
	// // 7 result variable + 1 extra for using.
	// tests.put( ElementBind.class, 8 );
	// // one for each table.
	// tests.put( ElementFilter.class, 2 );
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	// for (final Element el : results.get(ElementBind.class )) {
	// final ElementBind bind = (ElementBind) el;
	// assertTrue(q.getProjectVars().contains(bind.getVar()));
	// }
	// }
	//
	// @Test
	// public void testMethodParse() throws Exception {
	// final Query q = getQuery( "SELECT count( IntCol ) FROM foo");
	//
	// final VarExprList vLst = q.getProject();
	// assertEquals(1, vLst.getExprs().size());
	//
	// tests.put( ElementFilter.class, 1 );
	// validate( q, tests );
	//
	// }
	//
	// @Test
	// public void testMethodWithAliasParse() throws Exception {
	// final Query q = getQuery( "SELECT count( IntCol ) AS bar FROM foo");
	//
	// assertEquals(1, q.getProject().size());
	// assertEquals(1, q.getProjectVars().size());
	// assertEquals("bar", q.getProjectVars().get(0).getVarName());
	//
	// tests.put( ElementFilter.class, 1);
	// validate( q, tests );
	// }
	//
	// @Test
	// public void testNoColParse() throws Exception {
	// final String[] colNames = {
	// "StringCol", "NullableStringCol", "IntCol", "NullableIntCol"
	// };
	// final Query q = getQuery( "SELECT * FROM foo");
	//
	// final List<Var> vLst = q.getProjectVars();
	// assertEquals(4, vLst.size());
	// for (final Var v : vLst) {
	// assertTrue(Arrays.asList(colNames).contains(v.getName()));
	// }
	//
	// tests.put( ElementBind.class, 4 );
	// tests.put( ElementFilter.class, 1 );
	// tests.put( ElementOptional.class, 2);
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	// for (final Element el : results.get( ElementBind.class)) {
	// final ElementBind bind = (ElementBind) el;
	// assertTrue(q.getProjectVars().contains(bind.getVar()));
	// }
	//
	//
	// }
	//
	// @Test
	// public void testSpecColParse() throws Exception {
	// final Query q = getQuery( "SELECT StringCol FROM foo");
	//
	// final List<Var> vLst = q.getProjectVars();
	// assertEquals(1, vLst.size());
	// assertEquals(Var.alloc("StringCol"), vLst.get(0));
	//
	// tests.put( ElementBind.class, 1);
	// tests.put(ElementFilter.class,1);
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	// final ElementBind eb = (ElementBind) results.get(
	// ElementBind.class).get(0);
	// assertEquals("StringCol", eb.getVar().getName());
	// }
	//
	// @Test
	// public void testSpecColWithAliasParse() throws Exception {
	// final Query q = getQuery( "SELECT StringCol AS bar FROM foo");
	//
	// final List<Var> vLst = q.getProjectVars();
	// assertEquals(1, vLst.size());
	// assertEquals(Var.alloc("bar"), vLst.get(0));
	//
	// tests.put( ElementBind.class, 1);
	// tests.put(ElementFilter.class,1);
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	// final ElementBind eb = (ElementBind) results.get(
	// ElementBind.class).get(0);
	// assertEquals("bar", eb.getVar().getName());
	//
	// }
	//
	// @Test
	// public void testSpecColWithEqnParse() throws Exception {
	// final Query q = getQuery(
	// "SELECT StringCol FROM foo WHERE StringCol != 'baz'");
	//
	// tests.put( ElementBind.class, 1 );
	// tests.put( ElementFilter.class,2 );
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	// final List<Var> vLst = q.getProjectVars();
	// assertEquals(1, vLst.size());
	// assertEquals(Var.alloc("StringCol"), vLst.get(0));
	//
	// final ElementBind eb = (ElementBind) results.get(
	// ElementBind.class).get(0);
	// assertEquals("StringCol", eb.getVar().getName());
	//
	// // should be the last one
	// final Expr expr = ((ElementFilter) results.get(
	// ElementFilter.class).get(1))
	// .getExpr();
	// assertTrue(expr instanceof E_NotEquals);
	// final E_NotEquals expr2 = (E_NotEquals) expr;
	// assertEquals("StringCol",
	// ((ExprVar) (expr2.getArg1())).getVarName());
	// assertEquals("baz",
	// ((NodeValueString) (expr2.getArg2())).asString());
	// }
	//
	// @Test
	// public void testTableAliasParse() throws Exception {
	// final Query q = getQuery(
	// "SELECT StringCol FROM foo bar WHERE StringCol != 'baz'");
	//
	// final List<Var> vLst = q.getProjectVars();
	// assertEquals(1, vLst.size());
	// assertEquals(Var.alloc("StringCol"), vLst.get(0));
	//
	// tests.put( ElementBind.class, 1 );
	// tests.put( ElementFilter.class, 2 );
	// // main + 2 optional
	// tests.put( ElementPathBlock.class, 3 );
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	// final ElementBind eb = (ElementBind)
	// results.get(ElementBind.class).get(0);
	// assertEquals("StringCol", eb.getVar().getName());
	//
	// // should be the last one
	// final Expr expr = ((ElementFilter)
	// results.get(ElementFilter.class).get(1))
	// .getExpr();
	// assertTrue(expr instanceof E_NotEquals);
	// final E_NotEquals expr2 = (E_NotEquals) expr;
	// assertEquals("StringCol",
	// ((ExprVar) (expr2.getArg1())).getVarName());
	// assertEquals("baz",
	// ((NodeValueString) (expr2.getArg2())).asString());
	//
	//
	// final ElementPathBlock epb = (ElementPathBlock)
	// results.get(ElementPathBlock.class).get(0);
	// final Iterator<TriplePath> iter = epb.patternElts();
	// while (iter.hasNext()) {
	// final TriplePath t = iter.next();
	// assertTrue(t.getSubject().isVariable());
	// assertEquals(tableName.getGUID(), t.getSubject().getName());
	// }
	// }
	//
	// @Test
	// public void testTwoTableJoin() throws Exception {
	// final String[] columnNames = {
	// "foo" + NameUtils.SPARQL_DOT + "IntCol",
	// "foo" + NameUtils.SPARQL_DOT + "StringCol",
	// "foo" + NameUtils.SPARQL_DOT + "NullableStringCol",
	// "foo" + NameUtils.SPARQL_DOT + "NullableIntCol",
	// "bar" + NameUtils.SPARQL_DOT + "BarStringCol",
	// "bar" + NameUtils.SPARQL_DOT + "BarNullableStringCol",
	// "bar" + NameUtils.SPARQL_DOT + "BarIntCol",
	// "bar" + NameUtils.SPARQL_DOT + "NullableIntCol"
	// };
	// final Query q = getQuery(
	// "SELECT * FROM foo, bar WHERE foo.IntCol = bar.BarIntCol");
	//
	// final List<Var> vLst = q.getProjectVars();
	// assertEquals(columnNames.length, vLst.size());
	// for (final Var v : vLst) {
	// final ColumnName tn = ColumnName.getNameInstance("testCatalog",
	// "testSchema", "table", v.getName());
	// tn.setUsedSegments(NameSegments.FFTT);
	// assertTrue("missing " + tn.getSPARQLName(),
	// Arrays.asList(columnNames).contains(tn.getSPARQLName()));
	// }
	// tests.put( ElementBind.class, columnNames.length );
	// // one for each table + one real (join) filter
	// tests.put( ElementFilter.class, 3 );
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	//
	// final Expr expr = ((ElementFilter)
	// results.get(ElementFilter.class).get(2))
	// .getExpr();
	// assertTrue(expr instanceof E_Equals);
	// final E_Equals expr2 = (E_Equals) expr;
	// assertEquals("foo" + NameUtils.SPARQL_DOT + "IntCol",
	// ((ExprVar) (expr2.getArg1())).getVarName());
	// assertEquals("bar" + NameUtils.SPARQL_DOT + "BarIntCol",
	// ((ExprVar) (expr2.getArg2())).getVarName());
	// }
	//
	// @Test
	// public void testCountNullableColumn() throws Exception {
	// final Query q = getQuery( "select count(NullableIntCol) from foo");
	//
	// tests.put( ElementFilter.class, 1 );
	// validate( q, tests );
	// }
	//
	//
	// @Test
	// public void testColumnByNotNull() throws Exception {
	// final Query q = getQuery(
	// "select IntCol from foo WHERE NullableIntCol IS NULL");
	//
	// // one for table + one real filter
	// tests.put( ElementFilter.class, 2 );
	// tests.put( ElementBind.class, 2 );
	// Map<Class<? extends Element>,List<Element>> results = validate( q, tests
	// );
	//
	//
	// final Expr expr = ((ElementFilter)
	// results.get(ElementFilter.class).get(1))
	// .getExpr();
	// assertTrue(expr instanceof E_Bound);
	// assertEquals("v_39a47853_59fe_3101_9dc3_57a586635865", ((E_Bound)
	// expr).getArg().asVar().getName() );
	//
	// }
	//
	// @Test
	// public void testColumnEqualConst() throws ClassNotFoundException,
	// SQLException {
	// final List<String> colNames = getColumnNames("fooTable");
	// final ResultSet rset = stmt
	// .executeQuery("select * from foo where StringCol='Foo2String'");
	// int i = 0;
	// while (rset.next()) {
	// final StringBuilder sb = new StringBuilder();
	// for (final String colName : colNames) {
	// sb.append(String.format("[%s]=%s ", colName,
	// rset.getString(colName)));
	// }
	// final String s = sb.toString();
	// assertTrue(s.contains("[StringCol]=Foo2String"));
	// assertTrue(s.contains("[IntCol]=4"));
	// assertTrue(s
	// .contains("[type]=http://example.com/jdbc4sparql#fooTable"));
	// assertTrue(s.contains("[NullableStringCol]=null"));
	// assertTrue(s.contains("[NullableIntCol]=null"));
	// i++;
	// }
	// assertEquals(1, i);
	// rset.close();
	// stmt.close();
	// }
	//
	// @Test
	// public void testFullRetrieval() throws ClassNotFoundException,
	// SQLException {
	// final String[][] results = {
	// { "[StringCol]=FooString",
	// "[NullableStringCol]=FooNullableFooString",
	// "[NullableIntCol]=6", "[IntCol]=5",
	// "[type]=http://example.com/jdbc4sparql#fooTable" },
	// { "[StringCol]=Foo2String", "[NullableStringCol]=null",
	// "[NullableIntCol]=null", "[IntCol]=4",
	// "[type]=http://example.com/jdbc4sparql#fooTable" } };
	//
	// // get the column names.
	// final List<String> colNames = getColumnNames("fooTable");
	// final ResultSet rset = stmt.executeQuery("select * from fooTable");
	// int i = 0;
	// while (rset.next()) {
	// final List<String> lst = Arrays.asList(results[i]);
	// for (final String colName : colNames) {
	// lst.contains(String.format("[%s]=%s", colName,
	// rset.getString(colName)));
	// }
	// i++;
	// }
	// assertEquals(2, i);
	// rset.close();
	//
	// }
	//
	// @Test
	// public void testInnerJoinSelect() throws SQLException {
	// final ResultSet rset = stmt
	// .executeQuery("select fooTable.IntCol, barTable.IntCol, fooTable.StringCol, barTable.StringCol from fooTable inner join barTable ON fooTable.StringCol=barTable.StringCol");
	//
	// while (rset.next()) {
	// assertEquals(5, rset.getInt(1));
	// assertEquals(15, rset.getInt(2));
	// }
	// rset.close();
	// }
	//
	// @Test
	// public void testJoinSelect() throws SQLException {
	// final ResultSet rset = stmt
	// .executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable join barTable ON fooTable.StringCol=barTable.StringCol");
	//
	// while (rset.next()) {
	// assertEquals(5, rset.getInt(1));
	// assertEquals(15, rset.getInt(2));
	// }
	// rset.close();
	// }
	//
	// @Test
	// public void testMetadataQuery() throws Exception {
	// conn.setCatalog(MetaCatalogBuilder.LOCAL_NAME);
	// stmt.close();
	// stmt = conn.createStatement();
	// final ResultSet rset = stmt
	// .executeQuery("select tbl.* from Tables tbl");
	// while (rset.next()) {
	// rset.getString(1); // force a read.
	// }
	// rset.close();
	// }
	//
	// /*
	// * SELECT tbl.* FROM Online_Account AS tbl
	// */
	// @Test
	// public void testSelectAllTableAlias() throws Exception {
	// final String[][] results = {
	// { "[StringCol]=FooString",
	// "[NullableStringCol]=FooNullableFooString",
	// "[NullableIntCol]=6", "[IntCol]=5",
	// "[type]=http://example.com/jdbc4sparql#fooTable" },
	// { "[StringCol]=Foo2String", "[NullableStringCol]=null",
	// "[NullableIntCol]=null", "[IntCol]=5",
	// "[type]=http://example.com/jdbc4sparql#fooTable" } };
	//
	// // get the column names.
	// final List<String> colNames = getColumnNames("fooTable");
	// final ResultSet rset = stmt
	// .executeQuery("select tbl.* from fooTable tbl");
	// int i = 0;
	// while (rset.next()) {
	// final List<String> lst = Arrays.asList(results[i]);
	// for (final String colName : colNames) {
	// lst.contains(String.format("[%s]=%s", colName,
	// rset.getString(colName)));
	// }
	// i++;
	// }
	// assertEquals(2, i);
	// rset.close();
	// }
	//
	// @Test
	// public void testTableAlias() throws ClassNotFoundException, SQLException
	// {
	// final ResultSet rset = stmt
	// .executeQuery("select IntCol from fooTable tbl where StringCol='Foo2String'");
	//
	// assertTrue(rset.next());
	// assertEquals(5, rset.getInt("IntCol"));
	// assertFalse(rset.next());
	// rset.close();
	// }
	//
	// @Test
	// public void testWhereEqualitySelect() throws SQLException {
	// final ResultSet rset = stmt
	// .executeQuery("select fooTable.IntCol, barTable.IntCol from fooTable, barTable WHERE fooTable.StringCol=barTable.StringCol");
	//
	// while (rset.next()) {
	// assertEquals(5, rset.getInt(1));
	// assertEquals(15, rset.getInt(2));
	// }
	// rset.close();
	// }
	//
	// @Test
	// public void testCountFromTable() throws Exception {
	//
	// // count all the rows
	// final ResultSet rset = stmt
	// .executeQuery("select count(*) from fooTable");
	// final ResultSetMetaData rsm = rset.getMetaData();
	// assertEquals(1, rsm.getColumnCount());
	// rset.next();
	// assertEquals(2L, rset.getLong(1));
	// rset.close();
	//
	// }
	//
	// @Test
	// public void testCountFromTableWithEqn() throws Exception {
	//
	// // count one row
	// final ResultSet rset = stmt
	// .executeQuery("select count(*) from fooTable where StringCol='Foo2String'");
	// final ResultSetMetaData rsm = rset.getMetaData();
	// assertEquals(1, rsm.getColumnCount());
	// rset.next();
	// assertEquals(1L, rset.getLong(1));
	// rset.close();
	//
	// }
	//
	// @Test
	// public void testCountFunction() throws Exception {
	//
	// final String queryString =
	// "SELECT (count(*) as ?x) where { ?fooTable <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/jdbc4sparql#fooTable> }";
	// final Query query = QueryFactory.create(queryString);
	//
	// final List<QuerySolution> lqs = ((J4SConnection) conn).getCatalogs()
	// .get(conn.getCatalog()).executeLocalQuery(query);
	// assertEquals(1, lqs.size());
	// final QuerySolution qs = lqs.get(0);
	// assertEquals(3, qs.get("x").asLiteral().getValue());
	// }
	//
	// @Test
	// public void testCountWithAliasFromTable() throws Exception {
	// // count all the rows
	// final ResultSet rset = stmt
	// .executeQuery("select count(*) as junk from fooTable");
	// final ResultSetMetaData rsm = rset.getMetaData();
	// assertEquals(1, rsm.getColumnCount());
	// rset.next();
	// assertEquals(2L, rset.getLong(1));
	// assertEquals(2L, rset.getLong("junk"));
	// rset.close();
	//
	// stmt.close();
	// }
	//
	// @Test
	// public void testMinFromTable() throws Exception {
	//
	// final ResultSet rset = stmt
	// .executeQuery("select min( IntCol ) from fooTable");
	// final ResultSetMetaData rsm = rset.getMetaData();
	// assertEquals(1, rsm.getColumnCount());
	// rset.next();
	// assertEquals(4L, rset.getLong(1));
	// rset.close();
	// }
	//
	// @Test
	// public void testMinWithNullFromTable() throws Exception {
	// final ResultSet rset = stmt
	// .executeQuery("select min(NullableIntCol) from fooTable WHERE NullableIntCol IS NOT NULL ");
	// final ResultSetMetaData rsm = rset.getMetaData();
	// assertEquals(1, rsm.getColumnCount());
	// assertTrue(rset.next());
	// assertEquals(6L, rset.getLong(1));
	// rset.close();
	// }
	//
	// @Test
	// public void testSelectNotNull() throws Exception {
	// final ResultSet rset = stmt
	// .executeQuery("select IntCol from fooTable WHERE NullableIntCol IS NOT NULL ");
	// assertTrue(rset.next());
	// assertEquals(5, rset.getLong(1));
	// assertFalse(rset.next());
	// rset.close();
	// }
	//
	// @Test
	// public void testSelectNull() throws Exception {
	// final ResultSet rset = stmt
	// .executeQuery("select IntCol from fooTable WHERE NullableIntCol IS NULL ");
	// assertTrue(rset.next());
	// assertEquals(4, rset.getLong(1));
	// assertFalse(rset.next());
	// rset.close();
	// }
	//
	// @Test
	// public void testReadNull() throws Exception {
	// final ResultSet rset = stmt
	// .executeQuery("select NullableIntCol from fooTable");
	// final ResultSetMetaData rsm = rset.getMetaData();
	// assertEquals(1, rsm.getColumnCount());
	// while (rset.next()) {
	// if (rset.getLong(1) == 0) {
	// assertTrue(rset.wasNull());
	// }
	// }
	// rset.close();
	// }
	//
	// @Test
	// public void testCountWithNullFromTable() throws Exception {
	// final ResultSet rset = stmt
	// .executeQuery("select count(NullableIntCol) from fooTable");
	// final ResultSetMetaData rsm = rset.getMetaData();
	// assertEquals(1, rsm.getColumnCount());
	// rset.next();
	// assertEquals(1L, rset.getLong(1));
	// rset.close();
	//
	// }

}
