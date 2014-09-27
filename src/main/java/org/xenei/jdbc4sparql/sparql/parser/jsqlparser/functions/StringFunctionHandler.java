package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.util.Stack;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

import com.hp.hpl.jena.sparql.expr.Expr;

import net.sf.jsqlparser.expression.Function;

public class StringFunctionHandler extends AbstractFunctionHandler {
	public static final String[] STRING_FUNCTIONS = {};
			
	public StringFunctionHandler(SparqlQueryBuilder builder, Stack<Expr> stack) {
		super( builder );
	}

	@Override
	public boolean handle(Function func) { return false; }
}
