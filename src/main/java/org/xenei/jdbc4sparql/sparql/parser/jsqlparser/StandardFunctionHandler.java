package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.AbstractFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.AbstractFunctionHandler.FuncInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.NumericFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.StringFunctionHandler;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.SystemFunctionHandler;

public class StandardFunctionHandler {

	private final List<AbstractFunctionHandler> handlers;

	public StandardFunctionHandler(final SparqlQueryBuilder builder) {
		handlers = new ArrayList<AbstractFunctionHandler>();
		handlers.add(new NumericFunctionHandler(builder));
		handlers.add(new StringFunctionHandler(builder));
		handlers.add(new SystemFunctionHandler(builder));
	}

	public FuncInfo handle(final Function func, final String alias)
			throws SQLException {
		for (final AbstractFunctionHandler handler : handlers) {
			final FuncInfo funcInfo = handler.handle(func, alias);
			if (funcInfo != null) {
				return funcInfo;
			}
		}
		throw new IllegalArgumentException(String.format(
				"Function %s is not supported", func.getName()));
	}

}
