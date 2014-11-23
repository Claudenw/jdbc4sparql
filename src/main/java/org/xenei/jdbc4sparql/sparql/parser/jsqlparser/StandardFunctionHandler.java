package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import com.hp.hpl.jena.sparql.expr.Expr;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.AbstractFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.NumericFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.StringFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.SystemFunctionHandler;

public class StandardFunctionHandler {

	/**
	 * A function definition expressed as a column definition.
	 */
	public static class FunctionColumnDef implements ColumnDef {
		private final String columnClassName = "";
		private final int displaySize = 0;
		private final Integer type = null;
		private final int precision = 0;
		private final int scale = 0;
		private final boolean signed = false;
		private final int nullable = DatabaseMetaData.columnNoNulls;
		private String typeName;
		private final boolean autoIncrement = false;
		private final boolean caseSensitive = false;
		private final boolean currency = false;

		@Override
		public String getColumnClassName() {
			return columnClassName;
		}

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

	private final List<AbstractFunctionHandler> handlers;

	public StandardFunctionHandler(final SparqlQueryBuilder builder,
			final Stack<Expr> stack) {
		handlers = new ArrayList<AbstractFunctionHandler>();
		handlers.add(new NumericFunctionHandler(builder, stack));
		handlers.add(new StringFunctionHandler(builder, stack));
		handlers.add(new SystemFunctionHandler(builder, stack));
	}

	public ColumnDef handle(final Function func) throws SQLException {
		for (final AbstractFunctionHandler handler : handlers) {
			ColumnDef colDef = handler.handle(func);
			if (colDef != null) {
				return colDef;
			}
		}
		throw new IllegalArgumentException(String.format(
				"Function %s is not supported", func.getName()));
	}
}
