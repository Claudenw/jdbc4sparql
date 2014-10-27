package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import com.hp.hpl.jena.sparql.expr.Expr;

import java.util.Stack;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public class StringFunctionHandler extends AbstractFunctionHandler
{
	public static final String[] STRING_FUNCTIONS = {};

	public StringFunctionHandler( final SparqlQueryBuilder builder,
			final Stack<Expr> stack )
	{
		super(builder);
	}

	@Override
	public boolean handle( final Function func )
	{
		return false;
	}
}
