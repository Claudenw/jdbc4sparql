package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import com.hp.hpl.jena.sparql.expr.E_NumAbs;
import com.hp.hpl.jena.sparql.expr.E_NumCeiling;
import com.hp.hpl.jena.sparql.expr.E_NumFloor;
import com.hp.hpl.jena.sparql.expr.E_NumRound;
import com.hp.hpl.jena.sparql.expr.E_Random;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCount;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVar;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMax;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMaxDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMin;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMinDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggSum;
import com.hp.hpl.jena.sparql.expr.aggregate.AggSumDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;
import com.hp.hpl.jena.sparql.expr.aggregate.AggregatorBase;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
			"SUM", "ABS", "ROUND", "CEIL", "FLOOR", "RAND" };
	private static final int MAX = 0;
	private static final int MIN = 1;
	private static final int COUNT = 2;
	private static final int SUM = 3;
	private static final int ABS = 4;
	private static final int ROUND = 5;
	private static final int CEIL = 6;
	private static final int FLOOR = 7;
	private static final int RAND = 8;

	public NumericFunctionHandler( final SparqlQueryBuilder builder,
			final Stack<Expr> stack )
	{
		super(builder, stack);
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
				handleAggregate(AggMaxDistinct.class, AggMax.class, func);
				break;
			case MIN:
				handleAggregate(AggMinDistinct.class, AggMin.class, func);
				break;
			case COUNT:
				handleAggregate(AggCountDistinct.class, AggCount.class,
						AggCountVarDistinct.class, AggCountVar.class, func);
				break;
			case SUM:
				handleAggregate(AggSumDistinct.class, AggSum.class, func);
				break;
			case ABS:
				handleExpr1(E_NumAbs.class, func, java.sql.Types.NUMERIC);
				break;
			case ROUND:
				handleExpr1(E_NumRound.class, func, java.sql.Types.INTEGER);
				break;
			case CEIL:
				handleExpr1(E_NumCeiling.class, func, java.sql.Types.INTEGER);
				break;
			case FLOOR:
				handleExpr1(E_NumFloor.class, func, java.sql.Types.INTEGER);
				break;
			case RAND:
				handleExpr0(E_Random.class, func, java.sql.Types.NUMERIC);
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

	private void handleAggregate(
			final Class<? extends AggregatorBase> allDistinct,
			final Class<? extends AggregatorBase> all,
			final Class<? extends AggregatorBase> varDistinct,
			final Class<? extends AggregatorBase> var, final Function func )
			throws SQLException
	{
		Aggregator agg = null;
		final ExpressionList l = func.getParameters();
		if (l == null)
		{
			if (func.isAllColumns())
			{
				try
				{
					agg = func.isDistinct() ? allDistinct.newInstance() : all
							.newInstance();
				}
				catch (final InstantiationException e)
				{
					throw new IllegalStateException(e.getMessage(), e);
				}
				catch (final IllegalAccessException e)
				{
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
			else
			{
				throw getNoArgumentEx(func, "one");
			}
		}
		else if (l.getExpressions().size() > 1)
		{
			throw getToManyArgumentEx(func, "one");
		}
		else
		{
			final Expression expression = (Expression) l.getExpressions()
					.get(0);
			expression.accept(exprVisitor);
			try
			{
				final Constructor<? extends AggregatorBase> c = func
						.isDistinct() ? varDistinct.getConstructor(Expr.class)
						: var.getConstructor(Expr.class);
				agg = c.newInstance(exprVisitor.getResult());
			}
			catch (final NoSuchMethodException e)
			{
				throw new IllegalArgumentException(e.getMessage(), e);

			}
			catch (final SecurityException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
			catch (final InstantiationException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
			catch (final IllegalAccessException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
			catch (final InvocationTargetException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
		}
		stack.push(builder.register(agg, java.sql.Types.NUMERIC));
	}

	private void handleAggregate(
			final Class<? extends AggregatorBase> varDistinct,
			final Class<? extends AggregatorBase> var, final Function func )
			throws SQLException
	{
		Aggregator agg = null;
		final ExpressionList l = func.getParameters();
		if (l == null)
		{
			throw getNoArgumentEx(func, "one");
		}
		if (l.getExpressions().size() > 1)
		{
			throw getToManyArgumentEx(func, "one");
		}
		else
		{
			final Expression expression = (Expression) l.getExpressions()
					.get(0);
			expression.accept(exprVisitor);
			try
			{
				final Constructor<? extends AggregatorBase> c = func
						.isDistinct() ? varDistinct.getConstructor(Expr.class)
						: var.getConstructor(Expr.class);
				agg = c.newInstance(exprVisitor.getResult());
			}
			catch (final NoSuchMethodException e)
			{
				throw new IllegalArgumentException(e.getMessage(), e);

			}
			catch (final SecurityException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
			catch (final InstantiationException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
			catch (final IllegalAccessException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
			catch (final InvocationTargetException e)
			{
				throw new IllegalStateException(e.getMessage(), e);

			}
		}
		stack.push(builder.register(agg, java.sql.Types.NUMERIC));
	}
}
