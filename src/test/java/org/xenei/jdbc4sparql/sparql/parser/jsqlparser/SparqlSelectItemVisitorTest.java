package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.sparql.QueryInfoSet;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;

public class SparqlSelectItemVisitorTest {

	/**
	 * An iterator over the columns
	 *
	 */
	private static class ColumnIterator implements Answer<Iterator<Column>> {
		private final List<Column> lst;

		ColumnIterator(final List<Column> lst) {
			this.lst = lst;
		}

		@Override
		public Iterator<Column> answer(final InvocationOnMock invocation)
				throws Throwable {
			return lst.iterator();
		}
	}

	private SparqlSelectItemVisitor visitor;
	private SparqlQueryBuilder queryBuilder;

	private Query query;

	private QueryInfoSet queryInfoSet;
	private QueryColumnInfo columnInfo;

	private ColumnName columnName;

	private TableName tableName;
	private Map<String, Catalog> catalogs;
	private Catalog catalog;

	private CatalogName catalogName;
	private Schema schema;

	private SchemaName schemaName;

	private List<Column> colList;

	@Test
	public void allColumnsTest() throws SQLException {
		final AllColumns allColumns = new AllColumns();
		visitor.visit(allColumns);
		final List<Var> lst = query.getProjectVars();
		assertEquals(1, lst.size());
		assertEquals(GUIDObject.asVarName(columnName), lst.get(0).getName());
	}

	@Test
	public void allTableColumnsTest() throws SQLException {
		final net.sf.jsqlparser.schema.Table table = new net.sf.jsqlparser.schema.Table(
				"testSchema", "testTable");
		final AllTableColumns allTableColumns = new AllTableColumns(table);
		visitor.visit(allTableColumns);
		final List<Var> lst = query.getProjectVars();
		assertEquals(1, lst.size());
		assertEquals(GUIDObject.asVarName(columnName), lst.get(0).getName());
	}

	@Test
	public void selectExpressionTest() throws SQLException {
		final SelectExpressionItem selectExpression = new SelectExpressionItem();
		final net.sf.jsqlparser.schema.Table tbl = new net.sf.jsqlparser.schema.Table(
				"testSchema", "testTable");
		final net.sf.jsqlparser.schema.Column col = new net.sf.jsqlparser.schema.Column(
				tbl, "testCol");

		selectExpression.setExpression(col);

		visitor.visit(selectExpression);
		final List<Var> lv = query.getProjectVars();
		assertEquals(
				GUIDObject.asVar(columnName), lv.get(0));
	}

	@Before
	public void setup() throws Exception {
		colList = new ArrayList<Column>();

		catalogs = new HashMap<String, Catalog>();
		catalog = mock(Catalog.class);
		catalogName = new CatalogName("testCatalog");
		when(catalog.getName()).thenReturn(catalogName);
		when(catalog.getShortName()).thenReturn("testCatalog");
		catalogs.put("testCatalog", catalog);
		catalogs.put(VirtualCatalog.NAME, new VirtualCatalog());

		schema = mock(Schema.class);
		schemaName = catalogName.getSchemaName("testSchema");
		when(schema.getName()).thenReturn(schemaName);

		tableName = schemaName.getTableName("testTable");
		final Table table = mock(Table.class);
		when(table.getName()).thenReturn(tableName);
		when(table.getColumns()).thenAnswer(new ColumnIterator(colList));

		columnName = tableName.getColumnName("testCol");
		final Column column = mock(Column.class);
		when(column.getName()).thenReturn(columnName);
		when(table.getColumn(eq("testCol"))).thenReturn(column);
		colList.add(column);

		columnInfo = new QueryColumnInfo(column);

		queryInfoSet = new QueryInfoSet();
		queryInfoSet.addColumn(columnInfo);

		final SparqlParser parser = mock(SparqlParser.class);

		queryBuilder = new SparqlQueryBuilder(catalogs, parser, catalog, schema);
		queryBuilder.addTable(table, tableName, false);

		final Field f = SparqlQueryBuilder.class.getDeclaredField("query");
		f.setAccessible(true);
		;
		query = (Query) f.get(queryBuilder);

		visitor = new SparqlSelectItemVisitor(queryBuilder);
	}
}
