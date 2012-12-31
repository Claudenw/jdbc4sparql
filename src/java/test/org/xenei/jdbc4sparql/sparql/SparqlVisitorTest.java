package org.xenei.jdbc4sparql.sparql;

import java.io.StringReader;
import java.net.URL;
import java.sql.DatabaseMetaData;

import net.sf.jsqlparser.statement.Statement;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.meta.MetaColumn;
import org.xenei.jdbc4sparql.meta.MetaTableDef;
import org.xenei.jdbc4sparql.mock.MockCatalog;
import org.xenei.jdbc4sparql.mock.MockSchema;
import org.xenei.jdbc4sparql.mock.MockTable;
import org.xenei.jdbc4sparql.sparql.visitors.SparqlVisitor;

import net.sf.jsqlparser.parser.CCJSqlParserManager;

public class SparqlVisitorTest
{
	
	private CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private SparqlVisitor sv;
	
	@Before
	public void setUp() throws Exception
	{
		MockCatalog catalog = new MockCatalog();
		Schema schema = catalog.getSchemas().iterator().next();
		Table table = schema.getTable("foo");
		
		MetaTableDef tableDef = ((MockTable)table).getTableDef();
		tableDef.add( MetaColumn.getStringInstance("StringCol"));
		tableDef.add( MetaColumn.getStringInstance("NullableStringCol").setNullable(DatabaseMetaData.columnNullable ));
		tableDef.add( MetaColumn.getIntInstance("IntCol"));
		tableDef.add( MetaColumn.getIntInstance("NullableIntCol").setNullable(DatabaseMetaData.columnNullable ));
		sv = new SparqlVisitor( catalog );
		
	}
	
	@Test
	public void testSimpleParse() throws Exception
	{
		String query = "SELECT * FROM foo";
		Statement stmt =parserManager.parse(new StringReader(query));
		stmt.accept(sv);
		System.out.println( sv.getBuilder() );
		//System.out.println( sv.getQuery() );
	}
}
