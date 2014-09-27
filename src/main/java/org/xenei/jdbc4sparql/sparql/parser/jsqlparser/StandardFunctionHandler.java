package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.AbstractFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.NumericFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.StringFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.SystemFunctionHandler;

import net.sf.jsqlparser.expression.Function;
import com.hp.hpl.jena.sparql.expr.Expr;

public class StandardFunctionHandler {
	
	private final List<AbstractFunctionHandler> handlers;

	public StandardFunctionHandler(SparqlQueryBuilder builder,Stack<Expr> stack) {
		handlers = new ArrayList<AbstractFunctionHandler>();
		handlers.add( new NumericFunctionHandler( builder, stack) );
		handlers.add( new StringFunctionHandler( builder, stack) );
		handlers.add( new SystemFunctionHandler( builder, stack) );
	}
	
	public void handle(Function func) throws SQLException
	{
		for (AbstractFunctionHandler handler : handlers )
		{
			if (handler.handle(func))
			{
				return;
			}
		}
		throw new IllegalArgumentException( String.format("Function %s is not supported", func.getName() ));
	}
	
//	
//
//	
//	
//	public static class FunctionSchema implements Schema {
//
//		@Override
//		public String getName() {
//			return null;
//		}
//
//		@Override
//		public NameFilter<? extends Table> findTables(String tableNamePattern) {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public Catalog getCatalog() {
//			return null;
//		}
//
//		@Override
//		public Table getTable(String tableName) {
//			return new FunctionTable();
//		}
//
//		@Override
//		public Set<? extends Table> getTables() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//		
//	}
//	
//	public static class FunctionTable implements Table {
//
//		@Override
//		public String getName() {
//			return "Functions";
//		}
//
//		@Override
//		public void delete() {
//		}
//
//		@Override
//		public NameFilter<Column> findColumns(String columnNamePattern) {
//			return new NameFilter<Column>(columnNamePattern, Collections.<Column>emptyList());
//		}
//
//		@Override
//		public Catalog getCatalog() {
//			return null;
//		}
//
//		@Override
//		public Column getColumn(int idx) {
//			return null;
//		}
//
//		@Override
//		public Column getColumn(String name) {
//			return null;
//		}
//
//		@Override
//		public int getColumnCount() {
//			return 0;
//		}
//
//		@Override
//		public int getColumnIndex(Column column) {
//			return 0;
//		}
//
//		@Override
//		public int getColumnIndex(String columnName) {
//			return 0;
//		}
//
//		@Override
//		public Iterator<Column> getColumns() {
//			return Collections.emptyIterator();
//		}
//
//		@Override
//		public String getRemarks() {
//			return "Function Table";
//		}
//
//		@Override
//		public Schema getSchema() {
//			return null;
//		}
//
//		@Override
//		public String getSPARQLName() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public String getSQLName() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public Table getSuperTable() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public TableDef getTableDef() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public String getType() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public String getQuerySegmentFmt() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//
//		@Override
//		public boolean hasQuerySegments() {
//			// TODO Auto-generated method stub
//			return false;
//		}
//
//		@Override
//		public List getColumnList() {
//			// TODO Auto-generated method stub
//			return null;
//		}
//		
//	}
	
	public static class FunctionColumnDef implements ColumnDef {
		private String columnClassName = "";
		private int displaySize = 0;
		private Integer type = null;
		private int precision = 0;
		private int scale = 0;
		private boolean signed = false;
		private int nullable = DatabaseMetaData.columnNoNulls;
		private String typeName;
		private boolean autoIncrement = false;
		private boolean caseSensitive = false;
		private boolean currency = false;

		@Override
		public String getColumnClassName() {
			return columnClassName;		}

		@Override
		public int getDisplaySize() {
			return displaySize;
		}

		@Override
		public int getNullable() {
			return nullable;
		}

		@Override
		public int getPrecision() {
			return precision;
		}

		@Override
		public int getScale() {
			return scale;
		}

		@Override
		public int getType() {
			return type;
		}

		@Override
		public String getTypeName() {
			return typeName;
		}

		@Override
		public boolean isAutoIncrement() {
			return autoIncrement;
		}

		@Override
		public boolean isCaseSensitive() {
			return caseSensitive;
		}

		@Override
		public boolean isCurrency() {
			return currency;
		}

		@Override
		public boolean isDefinitelyWritable() {
			return false;
		}

		@Override
		public boolean isReadOnly() {
			return true;
		}

		@Override
		public boolean isSearchable() {
			return false;
		}

		@Override
		public boolean isSigned() {
			return signed;
		}

		@Override
		public boolean isWritable() {
			return false;
		}
		
	}
}
