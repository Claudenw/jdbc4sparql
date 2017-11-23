package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Arrays;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.AliasInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfoFactory;

import org.apache.jena.sparql.expr.E_NumAbs;
import org.apache.jena.sparql.expr.E_NumCeiling;
import org.apache.jena.sparql.expr.E_NumFloor;
import org.apache.jena.sparql.expr.E_NumRound;
import org.apache.jena.sparql.expr.E_Random;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;
import org.apache.jena.sparql.expr.aggregate.AggMax;
import org.apache.jena.sparql.expr.aggregate.AggMaxDistinct;
import org.apache.jena.sparql.expr.aggregate.AggMin;
import org.apache.jena.sparql.expr.aggregate.AggMinDistinct;
import org.apache.jena.sparql.expr.aggregate.AggSum;
import org.apache.jena.sparql.expr.aggregate.AggSumDistinct;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.expr.aggregate.AggregatorBase;

public class NumericFunctionHandler extends AbstractFunctionHandler {
	public static final String[] NUMERIC_FUNCTIONS = {
			"MAX", "MIN", "COUNT", "SUM", "ABS", "ROUND", "CEIL", "FLOOR",
			"RAND"
	};
	private static final int MAX = 0;
	private static final int MIN = 1;
	private static final int COUNT = 2;
	private static final int SUM = 3;
	private static final int ABS = 4;
	private static final int ROUND = 5;
	private static final int CEIL = 6;
	private static final int FLOOR = 7;
	private static final int RAND = 8;

	public NumericFunctionHandler(final SparqlQueryBuilder builder) {
		super(builder);
	}

	@Override
	public Expr handle(final Function func, final AliasInfo alias)
			throws SQLException {
		final int i = Arrays.asList(NumericFunctionHandler.NUMERIC_FUNCTIONS)
				.indexOf(func.getName().toUpperCase());
		switch (i) {
			case MAX:
				return handleAggregate(AggMaxDistinct.class, AggMax.class,
						func, alias);

			case MIN:
				return handleAggregate(AggMinDistinct.class, AggMin.class,
						func, alias);

			case COUNT:
				return handleAggregate(AggCountDistinct.class, AggCount.class,
						AggCountVarDistinct.class, AggCountVar.class, func,
						alias);
			case SUM:
				return handleAggregate(AggSumDistinct.class, AggSum.class,
						func, alias);

			case ABS:
				return handleExpr1(E_NumAbs.class, func,
						java.sql.Types.NUMERIC, alias);

			case ROUND:
				return handleExpr1(E_NumRound.class, func,
						java.sql.Types.INTEGER, alias);

			case CEIL:
				return handleExpr1(E_NumCeiling.class, func,
						java.sql.Types.INTEGER, alias);

			case FLOOR:
				return handleExpr1(E_NumFloor.class, func,
						java.sql.Types.INTEGER, alias);

			case RAND:
				return handleExpr0(E_Random.class, func,
						java.sql.Types.NUMERIC, alias);

			default:
				return null;
		}
	}

	private Expr handleAggregate(
			final Class<? extends AggregatorBase> allDistinct,
			final Class<? extends AggregatorBase> all,
			final Class<? extends AggregatorBase> varDistinct,
			final Class<? extends AggregatorBase> var, final Function func,
			final AliasInfo alias) throws SQLException {
		Aggregator agg = null;
		final ExpressionList l = func.getParameters();
		if (l == null) {
			if (func.isAllColumns()) {
				try {
					agg = func.isDistinct() ? allDistinct.newInstance() : all
							.newInstance();
				} catch (final InstantiationException e) {
					throw new IllegalStateException(e.getMessage(), e);
				} catch (final IllegalAccessException e) {
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
			else {
				throw getNoArgumentEx(func, "one");
			}
		}
		else if (l.getExpressions().size() > 1) {
			throw getToManyArgumentEx(func, "one");
		}
		else {
			final Expression expression = (Expression) l.getExpressions()
					.get(0);
			expression.accept(exprVisitor);
			try {
				final Constructor<? extends AggregatorBase> c = func
						.isDistinct() ? varDistinct.getConstructor(Expr.class)
								: var.getConstructor(Expr.class);
						agg = c.newInstance(exprVisitor.getResult());
			} catch (final NoSuchMethodException e) {
				throw new IllegalArgumentException(e.getMessage(), e);

			} catch (final SecurityException e) {
				throw new IllegalStateException(e.getMessage(), e);

			} catch (final InstantiationException e) {
				throw new IllegalStateException(e.getMessage(), e);

			} catch (final IllegalAccessException e) {
				throw new IllegalStateException(e.getMessage(), e);

			} catch (final InvocationTargetException e) {
				throw new IllegalStateException(e.getMessage(), e);

			}
		}
		final String aliasStr = alias.getAlias();
		final ExprAggregator expr = builder.register(agg,
				java.sql.Types.NUMERIC, aliasStr);
		return ExprInfoFactory.getInstance(expr, exprVisitor.getColumns(),
				getColumnName( alias ));

	}

	private Expr handleAggregate(
			final Class<? extends AggregatorBase> varDistinct,
			final Class<? extends AggregatorBase> var, final Function func,
			final AliasInfo alias) throws SQLException {
		Aggregator agg = null;
		final ExpressionList l = func.getParameters();
		if (l == null) {
			throw getNoArgumentEx(func, "one");
		}
		if (l.getExpressions().size() > 1) {
			throw getToManyArgumentEx(func, "one");
		}
		else {
			final Expression expression = (Expression) l.getExpressions()
					.get(0);
			expression.accept(exprVisitor);
			try {
				final Constructor<? extends AggregatorBase> c = func
						.isDistinct() ? varDistinct.getConstructor(Expr.class)
								: var.getConstructor(Expr.class);
						agg = c.newInstance(exprVisitor.getResult());
			} catch (final NoSuchMethodException e) {
				throw new IllegalArgumentException(e.getMessage(), e);

			} catch (final SecurityException e) {
				throw new IllegalStateException(e.getMessage(), e);

			} catch (final InstantiationException e) {
				throw new IllegalStateException(e.getMessage(), e);

			} catch (final IllegalAccessException e) {
				throw new IllegalStateException(e.getMessage(), e);

			} catch (final InvocationTargetException e) {
				throw new IllegalStateException(e.getMessage(), e);

			}
		}
		final String aliasStr = alias.getAlias();
		final ExprAggregator expr = builder.register(agg,
				java.sql.Types.NUMERIC, aliasStr);
		return ExprInfoFactory.getInstance(expr, exprVisitor.getColumns(),
				getColumnName( alias ));
	}
}
