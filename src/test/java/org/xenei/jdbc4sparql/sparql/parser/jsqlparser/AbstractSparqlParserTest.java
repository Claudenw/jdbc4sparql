package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.utils.ElementExtractor;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.impl.EntityManagerImpl;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_Function;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementBind;
import org.apache.jena.sparql.syntax.ElementPathBlock;

/**
 * 
 *
 */
abstract public class AbstractSparqlParserTest {

	protected Model model;
	protected SparqlParser parser;
	protected Map<String, Catalog> catalogs;
	protected RdfCatalog catalog;
	protected final static String CATALOG_NAME = "testCatalog";
	protected RdfSchema schema;
	protected final static String SCHEMA_NAME = "testSchema";
	protected final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	protected Query query;
	protected Map<Class<? extends Element>, Integer> tests;
	protected Map<Class<? extends Element>, Wrapper> results;
	protected TableName fooTableName;
	protected TableName barTableName;
	protected TableName virtualTableName = new TableName( CATALOG_NAME, VirtualSchema.NAME, VirtualTable.NAME);
	protected List<String> vars;

	protected static String RQD = "IntCol";
	protected static ColumnName RQD_NAME = new ColumnName( CATALOG_NAME, SCHEMA_NAME,
			"foo", RQD);
	protected static String OPT = "NullableStringCol";
	protected static ColumnName OPT_NAME = new ColumnName( CATALOG_NAME, SCHEMA_NAME,
			"foo", OPT);
	protected static String RQD_TEST = "StringCol";
	protected static String OPT_TEST = "NullableIntCol";

	@Before
	public void setup() {
		catalogs = new HashMap<String, Catalog>();
		catalogs.put(VirtualCatalog.NAME, new VirtualCatalog());
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("org.apache.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);

		model = ModelFactory.createDefaultModel();
		EntityManager em = EntityManagerFactory.create( model );
		final RDFConnection connection = RDFConnectionFactory.connect( DatasetFactory.create());
		
		catalog = new RdfCatalog.Builder().setLocalConnection(connection)
				.setName(CATALOG_NAME).build(em);
		catalogs.put(catalog.getShortName(), catalog);

		schema = new RdfSchema.Builder().setCatalog(catalog)
				.setName(SCHEMA_NAME).build();

		// create the foo table
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(
				MetaCatalogBuilder.getNonNullStringBuilder().build(
						em))
						.addColumnDef(
								MetaCatalogBuilder.getNullStringBuilder().build(em))
								.addColumnDef(
										MetaCatalogBuilder.getNonNullIntBuilder().build(em))
										.addColumnDef(
												MetaCatalogBuilder.getNullIntBuilder().build(em))
												.build(em);

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
		bldr.build();

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
		bldr.build();
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

	protected Query getQuery(final String sql) throws SQLDataException,
	JSQLParserException {
		final Statement stmt = parserManager.parse(new StringReader(sql));
		final SparqlVisitor sparqlVisitor = new SparqlVisitor(catalogs, parser,
				catalog, schema);
		stmt.accept(sparqlVisitor);
		return sparqlVisitor.getBuilder().build();
	}

	protected class Wrapper {
		List<Element> lst;
		Integer i;
	}

	protected Map<Class<? extends Element>, Wrapper> validate(final Query query,
			final Map<Class<? extends Element>, Integer> tests)
					throws IllegalAccessException, IllegalArgumentException,
					InvocationTargetException {
		return validate( query.getQueryPattern(), tests );
	}

	protected Map<Class<? extends Element>, Wrapper> validate(final Element query,
			final Map<Class<? extends Element>, Integer> tests)
					throws IllegalAccessException, IllegalArgumentException,
					InvocationTargetException {
		final Map<Class<? extends Element>, Wrapper> retval = new IdentityHashMap<Class<? extends Element>, Wrapper>();
		final ElementExtractor extractor = new ElementExtractor();
		for (final Map.Entry<Class<? extends Element>, Integer> test : tests
				.entrySet()) {
			query.visit(
					extractor.setMatchType(test.getKey()).reset());
			if (test.getValue() != null)
			{
				assertEquals("Wrong number of " + test.getKey(), test.getValue(),
					Integer.valueOf(extractor.getExtracted().size()));
			}
			final Wrapper wrapper = new Wrapper();
			wrapper.lst = extractor.getExtracted();
			wrapper.i = extractor.getExtracted().size();
			retval.put(test.getKey(), wrapper);
		}
		return retval;
	}
	
	protected void verifyVars(final List<Var> vars, final ItemName[] names) {
		assertEquals(names.length, vars.size());
		for (int i = 0; i < names.length; i++) {
			assertEquals(names[i].getSPARQLName(names[i].getUsedSegments()), vars.get(i).getName());
		}
	}

	protected List<String> verifyTable(final Wrapper w, final GUIDObject name) {
		final List<String> retval = new ArrayList<String>();
		for (final Element el : w.lst) {
			final ElementPathBlock epb = (ElementPathBlock) el;
			final Iterator<TriplePath> iter = epb.patternElts();
			while (iter.hasNext()) {
				final TriplePath t = iter.next();
				Assert.assertTrue(t.getSubject().isVariable());
				Assert.assertEquals(GUIDObject.asVarName(name), t.getSubject().getName());
				final Node n = t.getObject();
				if (n.isVariable()) {
					retval.add(t.getObject().getName());
				}
			}
		}
		return retval;
	}

	protected void verifyBinds(final Wrapper w, final List<String> cols) {
		for (final Element el : w.lst) {
			final ElementBind bind = (ElementBind) el;
			final Expr expr = bind.getExpr();
			assertTrue("Not an E_Function", expr instanceof E_Function);
			assertEquals(ForceTypeF.IRI, ((E_Function) expr).getFunctionIRI());
			final String name = ((E_Function) expr).getArg(1).getVarName();
			assertTrue("Missing " + name, cols.contains(name));
		}
	}

}
