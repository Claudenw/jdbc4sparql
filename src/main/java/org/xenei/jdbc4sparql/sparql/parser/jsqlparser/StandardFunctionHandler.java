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
	
	/**
	 * A function definition expressed as a column definition.
	 */
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
