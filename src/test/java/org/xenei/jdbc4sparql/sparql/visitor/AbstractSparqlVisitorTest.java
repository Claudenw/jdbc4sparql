package org.xenei.jdbc4sparql.sparql.visitor;

import static org.junit.Assert.assertEquals;
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
import org.xenei.jdbc4sparql.LoggingConfig;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.ForceTypeF;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlVisitor;
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
abstract public class AbstractSparqlVisitorTest {
	protected Map<String, Catalog> catalogs;
	protected SparqlParser parser;
	protected final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	protected SparqlVisitor sv;
	protected TableName tableName;
	protected Model model;
	protected RdfCatalog catalog;
	protected static final String CATALOG_NAME = "testCatalog";
	protected RdfSchema schema;
	protected static final String SCHEMA_NAME = "testSchema";
	protected static final TableName FOO_TABLE_NAME = new TableName( CATALOG_NAME, SCHEMA_NAME, "foo");
	protected static final TableName BAR_TABLE_NAME = new TableName( CATALOG_NAME, SCHEMA_NAME, "bar");
	
	protected Map<Class<? extends Element>, Integer> tests;
	protected Map<Class<? extends Element>, Wrapper> results;

	@Before
	public void setup() throws Exception {
		catalogs = new HashMap<String, Catalog>();
		catalogs.put(VirtualCatalog.NAME, new VirtualCatalog());
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("org.apache.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.DEBUG);
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		model = ModelFactory.createDefaultModel();
		RDFConnection connection = RDFConnectionFactory.connect( DatasetFactory.create(model) );
		EntityManager mgr = EntityManagerFactory.create( connection );
		catalog = new RdfCatalog.Builder().setLocalConnection(connection)
				.setName(CATALOG_NAME).build(mgr);
		catalogs.put(catalog.getShortName(), catalog);

		schema = new RdfSchema.Builder().setCatalog(catalog)
				.setName(SCHEMA_NAME).build( );

		// create the foo table
		final RdfTableDef tableDef = new RdfTableDef.Builder()
		.addColumnDef(
				MetaCatalogBuilder.getNonNullStringBuilder().build(
						mgr))
						.addColumnDef(
								MetaCatalogBuilder.getNullStringBuilder().build( mgr ))
								.addColumnDef(
										MetaCatalogBuilder.getNonNullIntBuilder().build( mgr ))
										.addColumnDef(
												MetaCatalogBuilder.getNullIntBuilder().build( mgr ))
												.build( mgr );

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
		bldr.build( );

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
		bldr.build( );
		tableName = bldr.getName();
		parser = new SparqlParserImpl();
		sv = new SparqlVisitor(catalogs, parser, catalog, schema);
		tests = new HashMap<Class<? extends Element>, Integer>();
	}

	protected Query getQuery(final String sql) throws SQLDataException,
			JSQLParserException {
		final String query = sql;
		final Statement stmt = parserManager.parse(new StringReader(query));

		stmt.accept(sv);
		return sv.getBuilder().build();
	}

	protected class Wrapper {
		List<Element> lst;
		Integer i;
	}

	protected Map<Class<? extends Element>, Wrapper> validate(
			final Query query,
			final Map<Class<? extends Element>, Integer> tests)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		return validate(query.getQueryPattern(), tests);
	}

	protected Map<Class<? extends Element>, Wrapper> validate(
			final Element query,
			final Map<Class<? extends Element>, Integer> tests)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		final Map<Class<? extends Element>, Wrapper> retval = new IdentityHashMap<Class<? extends Element>, Wrapper>();
		final ElementExtractor extractor = new ElementExtractor();
		for (final Map.Entry<Class<? extends Element>, Integer> test : tests
				.entrySet()) {
			query.visit(extractor.setMatchType(test.getKey()).reset());
			assertEquals("Wrong number of " + test.getKey(), test.getValue(),
					Integer.valueOf(extractor.getExtracted().size()));
			final Wrapper wrapper = new Wrapper();
			wrapper.lst = extractor.getExtracted();
			wrapper.i = extractor.getExtracted().size();
			retval.put(test.getKey(), wrapper);
		}
		return retval;
	}

	protected void verifyVars(final List<Var> vars, final String[] names) {
		assertEquals(names.length, vars.size());
		for (int i = 0; i < names.length; i++) {
			assertEquals(names[i], vars.get(i).getName());
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
				Assert.assertEquals(GUIDObject.asVar(name), t.getSubject());
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
