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
