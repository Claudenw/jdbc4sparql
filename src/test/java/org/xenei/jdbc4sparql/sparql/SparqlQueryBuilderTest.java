package org.xenei.jdbc4sparql.sparql;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import net.sf.jsqlparser.expression.Function;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.KeySegment;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.meta.MetaCatalogBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryItemCollection;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlParserImpl;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.FunctionColumn;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.FunctionColumnDef;
import org.xenei.jdbc4sparql.utils.ElementExtractor;
import org.xenei.jdbc4sparql.utils.ExpressionExtractor;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.E_UUID;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCount;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;

public class SparqlQueryBuilderTest {
	
	private Map<String,Catalog> catalogs;

	private SparqlParser parser;
	
	private SparqlQueryBuilder builder;

	private VirtualCatalog vCat;
	
	private Catalog catalog;
	private CatalogName catName;
	
	private Schema schema;
	private SchemaName sName;
	
	private Table table;
	private TableName tName;
	
	private Column column2;
	private ColumnName col2Name;
	
	private Column column;
	private ColumnName colName;
	
	private ColumnDef colDef;
	
	private List<Column> cols;
	
	@Before
	public void setup()
	{
		catName = new CatalogName( "TestCatalog");
		sName = catName.getSchemaName( "testSchema");
		tName = sName.getTableName("testTable");
		colName = tName.getColumnName("testColumn");
		col2Name = tName.getColumnName("testColumn2");

		parser = new SparqlParserImpl();
		catalog = mock( Catalog.class);
		when( catalog.getName() ).thenReturn( catName );
		when( catalog.getShortName() ).thenReturn( catName.getShortName());
		
		catalogs = new HashMap<String,Catalog>();
		catalogs.put( "TestCatalog", catalog );
		vCat = new VirtualCatalog();
		catalogs.put( VirtualCatalog.NAME, vCat);
		
		schema = mock(Schema.class);
		when( schema.getName() ).thenReturn( sName );
		
		table = mock(Table.class);
		when( table.getName() ).thenReturn( tName );
		when( table.hasQuerySegments()).thenReturn( true );
		when( table.getQuerySegmentFmt()).thenReturn( "%1$s <a> 'table' . ");
			
		colDef = mock(ColumnDef.class);
		when( colDef.getType()).thenReturn( java.sql.Types.VARCHAR);
		
		column = mock(Column.class);
		when( column.getName() ).thenReturn( colName );
		when( column.hasQuerySegments()).thenReturn( true );
		when( column.getQuerySegmentFmt()).thenReturn( "%1$s <of> %2$s . ");
		when( column.getColumnDef()).thenReturn( colDef );
		when( table.getColumn( eq( colName.getShortName()))).thenReturn( column );
		
		column2 = mock(Column.class);
		when( column2.getName() ).thenReturn( col2Name );
		when( column2.hasQuerySegments()).thenReturn( true );
		when( column2.getQuerySegmentFmt()).thenReturn( "%1$s <of> %2$s . ");
		when( column2.getColumnDef()).thenReturn( colDef );
		when( column2.isOptional()).thenReturn( true );
		when( table.getColumn( eq( col2Name.getShortName()))).thenReturn( column2 );
		
		cols = new ArrayList<Column>();
		cols.add( column );
		cols.add( column2 );
		when( table.getColumns()).thenAnswer( new ColumnAnswer( cols ) );
		when( table.getColumnList()).thenReturn( cols );
		
		builder = new SparqlQueryBuilder( catalogs, parser, catalog, schema );
	}
	
	private static SparqlQueryBuilder createBuilder( String tableName )
	{
		CatalogName catName = new CatalogName( "TestCatalog");
		SchemaName sName = catName.getSchemaName( "testSchema");
		TableName tName = sName.getTableName( tableName );
		ColumnName colName = tName.getColumnName("testColumn");
		ColumnName col2Name = tName.getColumnName("testColumn2");

		SparqlParserImpl parser = new SparqlParserImpl();
		RdfCatalog catalog = mock( RdfCatalog.class);
		when( catalog.getName() ).thenReturn( catName );
		when( catalog.getShortName() ).thenReturn( catName.getShortName());
		
		Map<String,Catalog> catalogs = new HashMap<String,Catalog>();
		catalogs.put( "TestCatalog", catalog );
		
		Schema schema = mock(Schema.class);
		when( schema.getName() ).thenReturn( sName );
		
		
		Table table = mock(Table.class);
		when( table.getName() ).thenReturn( tName );
		when( table.hasQuerySegments()).thenReturn(true);
		when( table.getQuerySegmentFmt()).thenReturn( "%1$s <a> 'table' . ");

			
		ColumnDef colDef = mock(ColumnDef.class);
		when( colDef.getType()).thenReturn( java.sql.Types.VARCHAR);
		
		Column column = mock(Column.class);
		when( column.getName() ).thenReturn( colName );
		when( column.hasQuerySegments()).thenReturn( true );
		when( column.getQuerySegmentFmt()).thenReturn( "%1$s <of> %2$s . ");
		when( column.getColumnDef()).thenReturn( colDef );
		when( table.getColumn( eq( colName.getShortName()))).thenReturn( column );
		
		Column column2 = mock(Column.class);
		when( column2.getName() ).thenReturn( col2Name );
		when( column2.hasQuerySegments()).thenReturn( true );
		when( column2.getQuerySegmentFmt()).thenReturn( "%1$s <of> %2$s . ");
		when( column2.getColumnDef()).thenReturn( colDef );
		when( column2.isOptional()).thenReturn( true );
		when( table.getColumn( eq( col2Name.getShortName()))).thenReturn( column2 );
		
		ArrayList<Column> cols = new ArrayList<Column>();
		cols.add( column );
		cols.add( column2 );
		when( table.getColumns()).thenAnswer( new ColumnAnswer( cols ) );
		
		SparqlQueryBuilder builder = new SparqlQueryBuilder( catalogs, parser, catalog, schema );
		builder.addTable( table, tName, false);
		return builder;
	}
	
	private static class ColumnAnswer implements Answer<Iterator<Column>> {
		List<Column> cols;
		ColumnAnswer( List<Column> cols )
		{
			this.cols = cols;
		}
		
		@Override
		public Iterator<Column> answer(InvocationOnMock invocation) throws Throwable {
			return cols.iterator();
		}
	}
	
	private Query getQuery() throws Exception
	{
		Field f = SparqlQueryBuilder.class.getDeclaredField("query");
		f.setAccessible(true);
		return (Query) f.get(builder);
	}
	
	private QueryInfoSet getInfoSet() throws Exception
	{
		Field f = SparqlQueryBuilder.class.getDeclaredField("infoSet");
		f.setAccessible(true);
		return (QueryInfoSet) f.get(builder);
	}
	
	@Test
	public void testAddTable_Required() throws Exception
	{
		builder.addTable( table,  tName, false );
	
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryTableInfo,Table,TableName> tableInfoList = infoSet.getTables();
		assertEquals( 1, tableInfoList.size() );
		QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals( tName, tableInfo.getName() );
		assertEquals( table, tableInfo.getTable() );
		assertEquals( false, tableInfo.isOptional());
		assertEquals( table.getName().getGUID(), tName.getGUID());
	}
	
	@Test
	public void testAddTable_WithAlias() throws Exception
	{
		TableName alias = new TableName( catName.getCatalog(), sName.getSchema(), "tableAlias");
		builder.addTable( table,  alias, false );
	
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryTableInfo,Table,TableName> tableInfoList = infoSet.getTables();
		assertEquals( 1, tableInfoList.size() );
		QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals( alias, tableInfo.getName() );
		assertEquals( table, tableInfo.getTable() );
		assertEquals( false, tableInfo.isOptional());
		assertEquals( table.getName().getGUID(), tName.getGUID());
		assertEquals( table.getName().getGUID(), alias.getGUID());
	}

	@Test
	public void testAddTable_Optional() throws Exception
	{
		builder.addTable( table,  tName, true );
	
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryTableInfo,Table,TableName> tableInfoList = infoSet.getTables();
		assertEquals( 1, tableInfoList.size() );
		QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals( tName, tableInfo.getName() );
		assertEquals( table, tableInfo.getTable() );
		assertEquals( true, tableInfo.isOptional());
		assertEquals( table.getName().getGUID(), tName.getGUID());
	}
	
	@Test
	public void testAddRequiredColumns() throws Exception
	{
		builder.addTable( table,  tName, false );
		builder.addRequiredColumns();
		
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryTableInfo,Table,TableName> tableInfoList = infoSet.getTables();
		assertEquals( 1, tableInfoList.size() );
		QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals( tName, tableInfo.getName() );
		assertEquals( table, tableInfo.getTable() );
		
		QueryItemCollection<QueryColumnInfo,Column,ColumnName> columnInfoList = infoSet.getColumns();
		assertEquals( 1, columnInfoList.size() );
		QueryColumnInfo columnInfo = columnInfoList.get(0);
		assertEquals( colName, columnInfo.getName() );
		assertEquals( column, columnInfo.getColumn() );
		
		Query query = getQuery();
		ElementExtractor extractor = new ElementExtractor( ElementPathBlock.class );
		query.getQueryPattern().visit(extractor);
		List<Element> lst = extractor.getExtracted();
		assertEquals( 1, lst.size() );

		Var tVar = Var.alloc( tName.getGUID());
		List<TriplePath> etb = ((ElementPathBlock) lst.get(0)).getPattern().getList();
		assertEquals( 2, etb.size() );
		TriplePath t = new TriplePath( new Triple( tVar, NodeFactory.createURI("a"), NodeFactory.createLiteral( "table")));
		assertTrue( etb.contains( t ));
		t = new TriplePath( new Triple( tVar, NodeFactory.createURI("of"), Var.alloc( colName.getGUID())));
		assertTrue( etb.contains( t ));
	}
	
	@Test
	public void testAddColumnToQuery() throws Exception
	{
		builder.addTable( table,  tName, false );
		builder.addRequiredColumns();
		builder.setSegmentCount();
		builder.addColumnToQuery(col2Name, true);
		QueryTableInfo tableInfo = builder.getTable(tName);
		builder.addVar( col2Name );
		
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryTableInfo,Table,TableName> tableInfoList = infoSet.getTables();
		assertEquals( 1, tableInfoList.size() );
		tableInfo = tableInfoList.get(0);
		assertEquals( tName, tableInfo.getName() );
		assertEquals( table, tableInfo.getTable() );
		
		QueryItemCollection<QueryColumnInfo,Column,ColumnName> columnInfoList = new QueryItemCollection<QueryColumnInfo,Column,ColumnName>(infoSet.getColumns());
		assertEquals( 2, columnInfoList.size() );
		
		assertTrue( columnInfoList.contains( colName ));
		assertTrue( columnInfoList.contains( column ));
		
		assertTrue( columnInfoList.contains( col2Name ));
		assertTrue( columnInfoList.contains( column2 ));

		Query query = builder.build();
		
		ElementExtractor extractor = new ElementExtractor( ElementPathBlock.class );
		query.getQueryPattern().visit(extractor);
		List<Element> lst = extractor.getExtracted();
		assertEquals( 2, lst.size() );

		List<TriplePath> etb = ((ElementPathBlock) lst.get(0)).getPattern().getList();
		assertEquals( 2, etb.size() );
		Var tVar = Var.alloc( tName.getGUID());
		TriplePath t = new TriplePath( new Triple( tVar, NodeFactory.createURI("a"), NodeFactory.createLiteral( "table")));
		assertTrue( etb.contains( t ));
		t = new TriplePath( new Triple( tVar, NodeFactory.createURI("of"), Var.alloc( colName.getGUID())));
		assertTrue( etb.contains( t ));
		
		etb = ((ElementPathBlock) lst.get(1)).getPattern().getList();
		assertEquals( 1, etb.size() );
		t = new TriplePath( new Triple( tVar, NodeFactory.createURI("of"), Var.alloc( col2Name.getGUID())));
		assertTrue( etb.contains( t ));
		
		query.getQueryPattern().visit( extractor.setMatchType( ElementFilter.class ).reset() );
		lst = extractor.getExtracted();
		assertEquals( 1, lst.size());
		
		ExpressionExtractor eExtractor = new ExpressionExtractor( CheckTypeF.class );
		((ElementFilter)lst.get(0)).getExpr().visit(eExtractor);
		List<Expr> eExpr = eExtractor.getExtracted();
		assertEquals( 2, eExpr.size());
		
		//ElementBind
		query.getQueryPattern().visit( extractor.setMatchType( ElementBind.class ).reset() );
		lst = extractor.getExtracted();
		assertEquals( 2, lst.size());
		
	}
	
	@Test
	public void testAddAlias() throws Exception
	{
		ColumnName alias = new ColumnName( "", "", "" ,"alias");
		alias.setUsedSegments( NameSegments.FFFT);
		
		builder.addTable( table,  tName, false );
		builder.addRequiredColumns();
		builder.setSegmentCount();
		builder.addAlias(col2Name, alias);
		builder.addVar( alias );
		
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryTableInfo,Table,TableName> tableInfoList = infoSet.getTables();
		assertEquals( 1, tableInfoList.size() );
		QueryTableInfo tableInfo = tableInfoList.get(0);
		assertEquals( tName, tableInfo.getName() );
		assertEquals( table, tableInfo.getTable() );
		
		QueryItemCollection<QueryColumnInfo,Column,ColumnName> columnInfoList = new QueryItemCollection<QueryColumnInfo,Column,ColumnName>(infoSet.getColumns());
		assertEquals( 3, columnInfoList.size() );
		
		assertTrue( columnInfoList.contains( colName ));
		assertTrue( columnInfoList.contains( column ));
		
		assertTrue( columnInfoList.contains( alias ));
		assertTrue( columnInfoList.contains( column2 ));

		assertTrue( columnInfoList.contains( col2Name ));
		
		Query query = builder.build();
		
		ElementExtractor extractor = new ElementExtractor( ElementBind.class );
		query.getQueryPattern().visit( extractor );
		List<Element> lst = extractor.getExtracted();
		assertEquals( 3, lst.size());
		List<String> sLst = new ArrayList<String>();
		for (Element e : lst )
		{
			sLst.add(((ElementBind)e).getVar().getName());
		}
		assertTrue( sLst.contains( "alias"));
	}
	
	@Test
	public void testAddFilter() throws Exception
	{
		Query query = getQuery();
		assertNull( query.getQueryPattern());
	
		Expr expr = new E_UUID();
		builder.addFilter(expr);
		
		ElementExtractor extractor = new ElementExtractor( ElementFilter.class );
		query.getQueryPattern().visit(extractor);
		List<Element> eLst = extractor.getExtracted();
		assertEquals( 1, eLst.size() );
	}
	
	@Test
	public void testAddColumn() throws Exception 
	{
		QueryColumnInfo columnInfo = null;
	
		try {
			columnInfo = builder.addColumn( col2Name, true);
			fail( "Should have thrown SQLException");
		}
		catch (SQLException e)
		{
			// expected
		}
		QueryTableInfo tableInfo = builder.addTable( table, tName, false );
		columnInfo = builder.addColumn( col2Name, true );
		assertNotNull( columnInfo );
		assertEquals( col2Name, columnInfo.getName());
		QueryInfoSet infoSet = getInfoSet();
		assertNotNull( infoSet.getColumn( col2Name ));
		Query query = getQuery();
		ElementExtractor extractor = new ElementExtractor( ElementOptional.class );
		query.getQueryPattern().visit(extractor);
		List<Element> eLst = extractor.getExtracted();
		assertEquals( 1, eLst.size() );
		eLst.get(0).visit( extractor.reset().setMatchType( ElementPathBlock.class ));
		eLst = extractor.getExtracted();
		assertEquals( 1, eLst.size() );
		ElementPathBlock epb = (ElementPathBlock) eLst.get(0);
		List<TriplePath> tLst = epb.getPattern().getList();
		assertEquals( 1, tLst.size() );
		Triple t = tLst.get(0).asTriple();
		Triple t2 = new Triple( tableInfo.getGUIDVar(), NodeFactory.createURI( "of" ), columnInfo.getGUIDVar() );
		assertEquals( t2, t);
	}
	
	@Test
	public void testAddGroupBy() throws Exception
	{
		Query query = getQuery();
		assertEquals(0, query.getGroupBy().size());
	
		Expr expr = new E_UUID();;
		builder.addGroupBy(expr);
		
		VarExprList eLst = query.getGroupBy();
		assertEquals( 1, eLst.size() );
		Var v = eLst.getVars().get(0);
		Expr e = eLst.getExpr( v );
		assertEquals( expr, e );
		
	}
	
	@Test
	public void testAddOrderBy() throws Exception
	{
		Query query = getQuery();
		assertNull( query.getOrderBy() );
		
		Expr expr = new E_UUID();
		builder.addOrderBy(expr, true);
		
		List<SortCondition> eLst = query.getOrderBy();
		assertEquals( 1, eLst.size() );
		SortCondition sc = eLst.get(0);
		assertEquals( expr, sc.getExpression() );
		assertEquals( Query.ORDER_ASCENDING, sc.getDirection());
	}
	
	@Test
	public void testAddUnion() throws Exception
	{
		List<SparqlQueryBuilder> builders = new ArrayList<SparqlQueryBuilder>();
		SparqlQueryBuilder builder1 = createBuilder( "table1" );
		builder1.addRequiredColumns();
		builder1.setAllColumns();
	
		SparqlQueryBuilder builder2 = createBuilder( "table2" );
		builder2.addRequiredColumns();
		builder2.setAllColumns();
		
		builders.add( builder1 );
		builders.add( builder2 );
		
		builder.addUnion(builders);
		
		Query query = getQuery();
		ElementExtractor extractor = new ElementExtractor( ElementUnion.class );
		query.getQueryPattern().visit(extractor);
		List<Element> lst = extractor.getExtracted();
		assertEquals( 1, lst.size() );
		
		query.getQueryPattern().visit( extractor.reset().setMatchType( ElementSubQuery.class ));
		lst = extractor.getExtracted();
		assertEquals( 2, lst.size() );
		
	}
	
	@Test
	public void testUsing() throws Exception
	{
		// setup a second table
		TableName tName2 = sName.getTableName("testTable2");
		

		
		Table table2 = mock(Table.class);
		when( table2.getName() ).thenReturn( tName2 );
		when( table2.getQuerySegmentFmt()).thenReturn( "%1$s <a> 'table' . ");
		when( table2.getColumn( eq( colName.getShortName()))).thenReturn( column );
		when( table2.getColumn( eq( col2Name.getShortName()))).thenReturn( column2 );
		
		cols = new ArrayList<Column>();
		cols.add( column );
		cols.add( column2 );
		when( table2.getColumns()).thenAnswer( new ColumnAnswer( cols ) );
		
		// setup complete
		
		builder.addTable( table, tName, false);
		builder.addTable( table2, tName2, false );
		builder.addUsing( colName.getShortName() );	
		builder.addRequiredColumns();
		builder.setAllColumns();
		
		Field f = SparqlQueryBuilder.class.getDeclaredField("columnsInUsing");
		f.setAccessible(true);
		List<String> inUsing = (List<String>) f.get(builder);
		assertEquals( 1, inUsing.size() );
		assertEquals( colName.getShortName(), inUsing.get(0));
		
		Query query = builder.build();
		
		//fail("incomplete Test");
	}
	
	@Test
	public void testAddVar_ColumnName() throws Exception 
	{
		Query query = getQuery();
		assertEquals( 0, query.getProjectVars().size() );
		
		builder.addTable( table, tName, true );
		builder.addVar( colName );
		
		List<Var> vLst = query.getProjectVars();
		assertEquals( 1,vLst.size());
		assertEquals( Var.alloc( colName.getSPARQLName()), vLst.get(0));
		
		VarExprList eLst = query.getProject();
		assertEquals( 1, eLst.size() );
		Var v = eLst.getVars().get(0);
		Expr e = eLst.getExpr( v );
		assertNull(  e );
		assertEquals(  Var.alloc( colName.getSPARQLName()), v );
	}
	
	@Test
	public void testAddVar_Expr_String() throws Exception
	{
		Query query = getQuery();
		assertEquals( 0, query.getProjectVars().size() );
		
		Expr expr = new E_UUID();
		builder.addVar( expr, "foo" );	
		
		List<Var> vLst = query.getProjectVars();
		assertEquals( 1,vLst.size());
		assertEquals( Var.alloc( "foo" ), vLst.get(0));
		
		VarExprList eLst = query.getProject();
		assertEquals( 1, eLst.size() );
		Var v = eLst.getVars().get(0);
		Expr e = eLst.getExpr( v );
		assertEquals( expr, e );
		assertEquals( Var.alloc( "foo" ), v );
	}
	
	
	@Test
	public void testAddVar_Expr_ColumnName() throws Exception
	{
		Query query = getQuery();
		assertEquals( 0, query.getProjectVars().size() );
		
		Expr expr = new E_UUID();
		ColumnName alias = new ColumnName( "", "", "" ,"alias");
		alias.setUsedSegments( NameSegments.FFFT );
		builder.registerFunctionColumn(alias, java.sql.Types.INTEGER);
		builder.addVar( expr, alias );
		
		
		List<Var> vLst = query.getProjectVars();
		assertEquals( 1,vLst.size());
		assertEquals( Var.alloc( alias.getSPARQLName() ), vLst.get(0));
		
		VarExprList eLst = query.getProject();
		assertEquals( 1, eLst.size() );
		Var v = eLst.getVars().get(0);
		Expr e = eLst.getExpr( v );
		assertEquals( expr, e );
		assertEquals( Var.alloc( alias.getSPARQLName() ), v );
	}
	
	@Test
	public void testRegisterFunctionColumn() throws Exception
	{
		ColumnName cName = new ColumnName( "","","","func");
		QueryColumnInfo columnInfo = builder.registerFunctionColumn(cName, java.sql.Types.INTEGER);
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryColumnInfo,Column,ColumnName> cols = infoSet.getColumns();
		assertEquals( 1, cols.size() );
		assertEquals( columnInfo, cols.get(0));
		Column col = cols.get(0).getColumn();
		assertTrue( col instanceof FunctionColumn );
		ColumnDef cd = col.getColumnDef();
		assertTrue( cd instanceof FunctionColumnDef );
		assertEquals( java.sql.Types.INTEGER, cd.getType());
		assertFalse( col.hasQuerySegments());
		assertNotNull( columnInfo.getTypeFilter() );
		assertNotNull( columnInfo.getDataFilter());

	}
	
	@Test
	public void testRegisterFunction() throws Exception
	{
		ColumnName cName = new ColumnName( "","","","func");
		QueryColumnInfo columnInfo = builder.registerFunction(cName, java.sql.Types.INTEGER);
		QueryInfoSet infoSet = getInfoSet();
		QueryItemCollection<QueryColumnInfo,Column,ColumnName> cols = infoSet.getColumns();
		assertEquals( 1, cols.size() );
		assertEquals( columnInfo, cols.get(0));
		Column col = cols.get(0).getColumn();
		assertTrue( col instanceof FunctionColumn );
		ColumnDef cd = col.getColumnDef();
		assertTrue( cd instanceof FunctionColumnDef );
		assertEquals( java.sql.Types.INTEGER, cd.getType());
		assertFalse( col.hasQuerySegments());
		assertNotNull( columnInfo.getTypeFilter() );
		assertNotNull( columnInfo.getDataFilter());

	}
	
	@Test
	public void testRegister() throws Exception
	{
		Aggregator agg = new AggCount();
		ExprAggregator expr = builder.register(agg, java.sql.Types.INTEGER);
		
		assertEquals( agg, expr.getAggregator());
		QueryInfoSet infoSet = getInfoSet();
		
		QueryItemCollection<QueryColumnInfo,Column,ColumnName> cols = infoSet.getColumns();
		assertEquals( 1, cols.size() );
		
		assertEquals( expr.getAggVar().getVarName(), "."+cols.get(0).getVar().getName());
		Column col = cols.get(0).getColumn();
		assertTrue( col instanceof FunctionColumn );
		ColumnDef cd = col.getColumnDef();
		assertTrue( cd instanceof FunctionColumnDef );
		assertEquals( java.sql.Types.INTEGER, cd.getType());
		assertFalse( col.hasQuerySegments());
		assertNotNull( cols.get(0).getTypeFilter() );
		assertNotNull( cols.get(0).getDataFilter());

	}
	
	@Test
	public void testGetCatalog()
	{
		Catalog cat = builder.getCatalog();
		assertNotNull( cat );
		assertEquals( catalog, cat );
	}
	
	@Test
	public void testGetCatalogName()
	{
		String s = builder.getCatalogName();
		assertNotNull( s );
		assertEquals( catName.getCatalog(), s );
	}
	
	@Test
	public void testGetCatalog_String()
	{
		Catalog cat = builder.getCatalog( VirtualCatalog.NAME );
		assertNotNull( cat );
		assertEquals( vCat, cat );
	}
	
	@Test
	public void testGetColumn_ColumnName()
	{
		builder.addTable( table, tName, false);
		builder.addRequiredColumns();
		QueryColumnInfo columnInfo = builder.getColumn(colName);
		assertNotNull( columnInfo );
		assertEquals( colName, columnInfo.getName() );
		assertEquals( column, columnInfo.getColumn() );
	}
	
	@Test
	public void testGetColumnCount() throws Exception
	{
		builder.addTable( table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
		builder.build();
		assertEquals( 2, builder.getColumnCount() );
	}
	
	@Test
	public void testGetColumn_Int() throws Exception
	{
		builder.addTable( table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
		builder.build();
		QueryColumnInfo columnInfo = builder.getColumn(0);
		assertNotNull( columnInfo );
		assertEquals( colName.getGUID(), columnInfo.getGUID() );
		assertEquals( column, columnInfo.getColumn() );
	}
	
	@Test
	public void testGetColumnIndex() throws Exception
	{
		builder.addTable( table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
		builder.build();
		int i= builder.getColumnIndex( colName.getSPARQLName() );
		assertEquals( 0, i );
		
	}
	
	@Test
	public void testGetDefaultSchemaName()
	{
		String s = builder.getDefaultSchemaName();
		assertNotNull( s );
		assertEquals( sName.getShortName(), s );
	}
	
	@Test
	public void testGetDefaultSchema()
	{
		Schema s = builder.getDefaultSchema();
		assertNotNull( s );
		assertEquals( schema, s );
	}
	
	@Test
	public void testGetDefaultTableName()
	{
		String s = builder.getDefaultTableName();
		assertNull( s );
		builder.addTable(table, tName, false);
		s = builder.getDefaultTableName();
		assertNotNull( s );
		assertEquals( table.getName().getShortName(), s );

		// create a second table
		Table table2 = mock(Table.class);
		when( table2.getName() ).thenReturn( sName.getTableName("table2") );
		when( table2.hasQuerySegments()).thenReturn(false);
		builder.addTable(table2, table2.getName(), false);
		
		s = builder.getDefaultTableName();
		assertNull( s );
	}
	
	@Test
	public void testGetSegments()
	{
		NameSegments s = builder.getSegments();
		assertNotNull( s );
		assertEquals(  NameSegments.ALL, s );
		
		builder.addTable( table, tName, false );
		s = builder.getSegments();
		assertNotNull( s );
		assertEquals(  NameSegments.ALL, s );
		
		builder.addColumnToQuery(colName, false);
		s = builder.getSegments();
		assertNotNull( s );
		assertEquals(  NameSegments.ALL, s );
		
		builder.setSegmentCount();
		s = builder.getSegments();
		assertNotNull( s );
		assertEquals( NameSegments.FFFT, s );
		
	}
	
	@Test
	public void testGetTable()
	{
		assertNull( builder.getTable( tName ) );
		builder.addTable( table, tName, false);
		QueryTableInfo tableInfo = builder.getTable( tName );
		assertNotNull( tableInfo );
		assertEquals( tName, tableInfo.getName() );
		assertEquals( table, tableInfo.getTable() );
	}
	
	@Test
	public void testSetAllColumns() throws Exception
	{
		builder.addTable( table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
	
		QueryInfoSet infoSet = getInfoSet();
		assertEquals( 2, infoSet.getColumns().size() );
		Iterator<Column> cols = table.getColumns();
		while (cols.hasNext())
		{
			assertNotNull(infoSet.getColumn( cols.next().getName() ));
		}
		
	}
	
	@Test
	public void testSetDistinct() throws Exception
	{
		Query query = getQuery();
		assertFalse( query.isDistinct());
		builder.setDistinct();
		assertTrue( query.isDistinct() );
	}
	
	
	@Test
	public void testSetHaving() throws Exception
	{
		Query query = getQuery();
		Expr expr = new E_UUID();
		builder.setHaving( expr );
		assertNotNull( query.getHavingExprs() );
		assertEquals( 1, query.getHavingExprs().size() );
		assertEquals( expr, query.getHavingExprs().get(0));
	}
	
	@Test
	public void testSetKey() throws Exception
	{
		builder.addTable( table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
	
		final KeySegment segment = new KeySegment() {

			@Override
			public int compare(Object[] o1, Object[] o2) {
				return Utils.compare( getIdx(), isAscending(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKeySegment";
			}

			@Override
			public short getIdx() {
				return 0;
			}

			@Override
			public boolean isAscending() {
				return true;
			}};
		
		
		Key key = new Key<KeySegment>() {

			@Override
			public int compare(Object[] o1, Object[] o2) {
				return Utils.compare(getSegments(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKey";
			}

			@Override
			public String getKeyName() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public List<KeySegment> getSegments() {
				return Arrays.asList( new KeySegment[] { segment });
			}

			@Override
			public boolean isUnique() {
				return true;
			}};
			
		
		builder.setKey( key );
		Query query = getQuery();
		assertTrue( query.isDistinct() );
		List<SortCondition> lst = query.getOrderBy();
		assertEquals( 1,  lst.size() );
		SortCondition sc = lst.get(0);
		assertEquals( Query.ORDER_ASCENDING, sc.getDirection());
		assertEquals( new ExprVar( colName.getSPARQLName() ), sc.getExpression() );
		
	}
	
	@Test
	public void testSetLimit() throws Exception
	{
		Query query = getQuery();
		assertEquals( Query.NOLIMIT, query.getLimit());
		builder.setLimit(5L);
		assertEquals( 5L, query.getLimit());
		builder.setLimit(Query.NOLIMIT );
		assertEquals( Query.NOLIMIT, query.getLimit());
	}
	
	@Test
	public void testSetOffset() throws Exception
	{
		Query query = getQuery();
		assertEquals( Query.NOLIMIT, query.getOffset());
		builder.setOffset(5L);
		assertEquals( 5L, query.getOffset());
		builder.setOffset(Query.NOLIMIT );
		assertEquals( Query.NOLIMIT, query.getOffset());
	}
	
	@Test
	public void testSetOrderBy() throws Exception
	{
		builder.addTable( table, tName, false);
		builder.addRequiredColumns();
		builder.setAllColumns();
	
		final KeySegment segment = new KeySegment() {

			@Override
			public int compare(Object[] o1, Object[] o2) {
				return Utils.compare( getIdx(), isAscending(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKeySegment";
			}

			@Override
			public short getIdx() {
				return 0;
			}

			@Override
			public boolean isAscending() {
				return true;
			}};
		
		
		Key key = new Key<KeySegment>() {

			@Override
			public int compare(Object[] o1, Object[] o2) {
				return Utils.compare(getSegments(), o1, o2);
			}

			@Override
			public String getId() {
				return "TestKey";
			}

			@Override
			public String getKeyName() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public List<KeySegment> getSegments() {
				return Arrays.asList( new KeySegment[] { segment });
			}

			@Override
			public boolean isUnique() {
				return true;
			}};
			
		
		builder.setOrderBy( key );
		Query query = getQuery();
		assertFalse( query.isDistinct() );
		List<SortCondition> lst = query.getOrderBy();
		assertEquals( 1,  lst.size() );
		SortCondition sc = lst.get(0);
		assertEquals( Query.ORDER_ASCENDING, sc.getDirection());
		assertEquals( new ExprVar( colName.getSPARQLName() ), sc.getExpression() );
		
	}
	
	@Test
	public void testSetGUID() throws Exception
	{
		QueryInfoSet infoSet = getInfoSet();
		assertFalse( infoSet.useGUID() );
		builder.setUseGUID( true );
		assertTrue( infoSet.useGUID() );
		builder.setUseGUID( false );
		assertFalse( infoSet.useGUID() );
		
	}
	
	@Test
	public void testGetColumn_Node() throws Exception
	{
		Var n = Var.alloc( "testColumn" );
		try {
			builder.getColumn( n );
			fail( "Should have thrown IllegalArgumentException");
		}
		catch (IllegalArgumentException expected)
		{
			
		}
		QueryTableInfo tableInfo = builder.addTable( table, tName, false );
		builder.addTableColumns(tableInfo);
		builder.setAllColumns();
		// reset for size
		QueryColumnInfo columnInfo = builder.getColumn( n );
		assertEquals( colName.getSPARQLName(), columnInfo.getName().getSPARQLName());
		assertEquals( column, columnInfo.getColumn());
		
	}
	
	@Test
	public void testGetTable_Node() throws Exception
	{
		Var n = Var.alloc( "testSchema"+NameUtils.SPARQL_DOT+"testTable" );
		assertNull( builder.getTable( n ));
		
		QueryTableInfo tableInfo = builder.addTable( table, tName, false );
		builder.addTableColumns(tableInfo);
		builder.setAllColumns();
		// reset for size
		QueryTableInfo tableInfo2 = builder.getTable( n );
		assertEquals( tableInfo, tableInfo2 );
		assertEquals( tName, tableInfo2.getName());
		assertEquals( table, tableInfo2.getTable());
		
	}

	@Test
	public void testGetResultColumns() throws SQLException
	{
		List<QueryColumnInfo> lst = builder.getResultColumns();
		assertNotNull( lst );
		assertTrue( lst.isEmpty() );
		
		QueryTableInfo tableInfo = builder.addTable( table, tName, false );
		builder.addTableColumns(tableInfo);
		builder.setAllColumns();
//		lst = builder.getResultColumns();
//		assertTrue( lst.isEmpty() );
//		
//		builder.build();
		lst = builder.getResultColumns();
		assertEquals( 2, lst.size() );
		
	}
	
}
