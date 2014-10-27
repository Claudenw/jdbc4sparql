package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCount;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVar;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMax;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMin;
import com.hp.hpl.jena.sparql.expr.aggregate.AggSum;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public class NumericFunctionHandler extends AbstractFunctionHandler
{
	public static final String[] NUMERIC_FUNCTIONS = { "MAX", "MIN", "COUNT",
			"SUM" };
	private static final int MAX = 0;
	private static final int MIN = 1;
	private static final int COUNT = 2;
	private static final int SUM = 3;
	private final Stack<Expr> stack;

	public NumericFunctionHandler( final SparqlQueryBuilder builder,
			final Stack<Expr> stack )
	{
		super(builder);
		this.stack = stack;
	}

	private IllegalArgumentException getNoArgumentEx( final Function func )
	{
		return new IllegalArgumentException(String.format(
				"No arguments provided to %s function, one expected", func
						.getName().toUpperCase()));
	}

	private IllegalArgumentException getToManyArgumentEx( final Function func )
	{
		return new IllegalArgumentException(String.format(
				"To many arguments provided to %s function, one expected", func
						.getName().toUpperCase()));
	}

	@Override
	public boolean handle( final Function func ) throws SQLException
	{
		final int stackCheck = stack.size();
		final int i = Arrays.asList(NumericFunctionHandler.NUMERIC_FUNCTIONS)
				.indexOf(func.getName().toUpperCase());
		switch (i)
		{
			case MAX:
				handleMax(func);
				break;
			case MIN:
				handleMin(func);
				break;
			case COUNT:
				handleCount(func);
				break;
			case SUM:
				handleSum(func);
				break;
			default:
				return false;
		}
		if (stack.size() != (stackCheck + 1))
		{
			throw new IllegalStateException(String.format(
					"Expected %s items on stack, found %s", stackCheck + 1,
					stack.size()));
		}
		return true;
	}

	private void handleCount( final Function func ) throws SQLException
	{
		Aggregator agg = null;
		final ExpressionList l = func.getParameters();
		if (l == null)
		{
			if (func.isAllColumns())
			{
				agg = new AggCount();
			}
			else
			{
				throw getNoArgumentEx(func);
			}
		}
		else if (l.getExpressions().size() > 1)
		{
			throw getToManyArgumentEx(func);
		}
		else
		{
			final Expression expression = (Expression) l.getExpressions()
					.get(0);
			expression.accept(exprVisitor);
			agg = new AggCountVar(exprVisitor.getResult());
		}
		stack.push(builder.register(agg, java.sql.Types.NUMERIC));
	}

	private void handleMax( final Function func )
	{
		final ExpressionList l = func.getParameters();
		if (l == null)
		{
			throw getNoArgumentEx(func);
		}
		if (l.getExpressions().size() > 1)
		{
			throw getToManyArgumentEx(func);
		}
		final Expression expression = (Expression) l.getExpressions().get(0);
		expression.accept(exprVisitor);
		new AggMax(exprVisitor.getResult());

	}

	private void handleMin( final Function func )
	{
		final ExpressionList l = func.getParameters();
		if (l == null)
		{
			throw getNoArgumentEx(func);
		}
		if (l.getExpressions().size() > 1)
		{
			throw getToManyArgumentEx(func);
		}
		final Expression expression = (Expression) l.getExpressions().get(0);
		expression.accept(exprVisitor);
		new AggMin(exprVisitor.getResult());
	}

	private void handleSum( final Function func )
	{
		final ExpressionList l = func.getParameters();
		if (l == null)
		{
			throw getNoArgumentEx(func);
		}
		if (l.getExpressions().size() > 1)
		{
			throw getToManyArgumentEx(func);
		}
		final Expression expression = (Expression) l.getExpressions().get(0);
		expression.accept(exprVisitor);
		new AggSum(exprVisitor.getResult());
	}
}
