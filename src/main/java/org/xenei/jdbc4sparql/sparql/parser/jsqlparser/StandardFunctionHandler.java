package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.AliasInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.AbstractFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.NumericFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.StringFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.SystemFunctionHandler;

import org.apache.jena.sparql.expr.Expr;

public class StandardFunctionHandler {

	private final List<AbstractFunctionHandler> handlers;

	public StandardFunctionHandler(final SparqlQueryBuilder builder) {
		handlers = new ArrayList<AbstractFunctionHandler>();
		handlers.add(new NumericFunctionHandler(builder));
		handlers.add(new StringFunctionHandler(builder));
		handlers.add(new SystemFunctionHandler(builder));
	}

	public Expr handle(final Function func, final AliasInfo alias)
			throws SQLException {
		for (final AbstractFunctionHandler handler : handlers) {
			final Expr exprInfo = handler.handle(func, alias);
			if (exprInfo != null) {
				return exprInfo;
			}
		}
		throw new IllegalArgumentException(String.format(
				"Function %s is not supported", func.getName()));
	}

}
