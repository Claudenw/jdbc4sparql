package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.vocabulary.RDF;

import java.io.StringReader;
import java.sql.DatabaseMetaData;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.impl.TableDefImpl;
import org.xenei.jdbc4sparql.meta.MetaColumn;
import org.xenei.jdbc4sparql.mock.MockCatalog;
import org.xenei.jdbc4sparql.mock.MockSchema;
import org.xenei.jdbc4sparql.sparql.visitors.SparqlVisitor;

public class SparqlVisitorTest
{

	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private SparqlVisitor sv;

	@Before
	public void setUp() throws Exception
	{
		final MockCatalog catalog = new MockCatalog();
		final MockSchema schema = (MockSchema) catalog
				.getSchema(MockSchema.LOCAL_NAME);
		final TableDefImpl tableDef = new TableDefImpl("foo");
		tableDef.add(MetaColumn.getStringInstance("StringCol"));
		tableDef.add(MetaColumn.getStringInstance("NullableStringCol")
				.setNullable(DatabaseMetaData.columnNullable));
		tableDef.add(MetaColumn.getIntInstance("IntCol"));
		tableDef.add(MetaColumn.getIntInstance("NullableIntCol").setNullable(
				DatabaseMetaData.columnNullable));
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

		final Element e = q.getQueryPattern();
		Assert.assertTrue(e instanceof ElementGroup);
		final ElementGroup eg = (ElementGroup) e;
		final List<Element> eLst = eg.getElements();
		Assert.assertEquals(5, eLst.size());
		List<Var> vLst = q.getProjectVars();
		Assert.assertEquals( 5,  vLst.size() );
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
		Assert.assertEquals(1, eLst.size());
		Assert.assertTrue(eLst.get(0) instanceof ElementTriplesBlock);
		final ElementTriplesBlock etb = (ElementTriplesBlock) eLst.get(0);
		final List<Triple> tLst = etb.getPattern().getList();
		Assert.assertTrue(tLst.contains(new Triple(Node
				.createVariable("TABLE_MockCatalog_MockSchema_foo"), RDF.type
				.asNode(), Node
				.createURI("http://org.xenei.jdbc4sparql/meta#foo"))));
		Assert.assertTrue(tLst.contains(new Triple(Node
				.createVariable("TABLE_MockCatalog_MockSchema_foo"), Node
				.createURI("http://org.xenei.jdbc4sparql/meta#StringCol"), Node
				.createVariable("StringCol"))));
		List<Var> vLst = q.getProjectVars();
		Assert.assertEquals( 1,  vLst.size() );
		Assert.assertEquals( Var.alloc("StringCol"), vLst.get(0));

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
		Assert.assertTrue(eLst.get(0) instanceof ElementTriplesBlock);
		final ElementTriplesBlock etb = (ElementTriplesBlock) eLst.get(0);
		final List<Triple> tLst = etb.getPattern().getList();
		Assert.assertTrue(tLst.contains(new Triple(Node
				.createVariable("TABLE_MockCatalog_MockSchema_foo"), RDF.type
				.asNode(), Node
				.createURI("http://org.xenei.jdbc4sparql/meta#foo"))));
		Assert.assertTrue(tLst.contains(new Triple(Node
				.createVariable("TABLE_MockCatalog_MockSchema_foo"), Node
				.createURI("http://org.xenei.jdbc4sparql/meta#StringCol"), Node
				.createVariable("StringCol"))));
		Assert.assertTrue(eLst.get(1) instanceof ElementFilter);
		Assert.assertEquals( "FILTER ( ?StringCol != \"baz\" )", eLst.get(1).toString());
		List<Var> vLst = q.getProjectVars();
		Assert.assertEquals( 1,  vLst.size() );
		Assert.assertEquals( Var.alloc("StringCol"), vLst.get(0));

	}
}
