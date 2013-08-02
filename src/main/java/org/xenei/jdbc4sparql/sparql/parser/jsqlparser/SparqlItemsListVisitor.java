package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import com.hp.hpl.jena.sparql.expr.ExprList;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public class SparqlItemsListVisitor implements ItemsListVisitor
{

	private final SparqlExprVisitor exprVisitor;
	private ExprList result;

	SparqlItemsListVisitor( final SparqlQueryBuilder builder )
	{
		exprVisitor = new SparqlExprVisitor(builder);
	}

	public ExprList getResult()
	{
		return result;
	}

	@Override
	public void visit( final ExpressionList expressionList )
	{
		final List<Expression> l = expressionList.getExpressions();
		result = new ExprList();
		// accept them in reverse order
		for (final Expression e : l)
		{
			e.accept(exprVisitor);
			result.add(exprVisitor.getResult());
		}
	}

	@Override
	public void visit( final SubSelect subSelect )
	{
		subSelect.accept(exprVisitor);
	}

}
