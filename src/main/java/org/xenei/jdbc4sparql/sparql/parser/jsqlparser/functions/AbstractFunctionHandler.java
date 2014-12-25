package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.function.FunctionEnv;
import com.hp.hpl.jena.sparql.graph.NodeTransform;

public abstract class AbstractFunctionHandler {
	protected SparqlQueryBuilder builder;
	protected final SparqlExprVisitor exprVisitor;
	protected final TableName tblName;

	public AbstractFunctionHandler(final SparqlQueryBuilder builder) {
		this.builder = builder;
		this.exprVisitor = new SparqlExprVisitor(builder,
				SparqlQueryBuilder.REQUIRED);
		tblName = new TableName(VirtualCatalog.NAME, VirtualSchema.NAME,
				VirtualTable.NAME);
		builder.getTable(tblName);
	}

	protected IllegalArgumentException getNoArgumentEx(final Function func,
			final String count) {
		return new IllegalArgumentException(String.format(
				"No arguments provided to %s function, %s expected", func
				.getName().toUpperCase(), count));
	}

	protected IllegalArgumentException getToManyArgumentEx(final Function func,
			final String count) {
		return new IllegalArgumentException(String.format(
				"To many arguments provided to %s function, %s expected", func
				.getName().toUpperCase(), count));
	}

	protected IllegalArgumentException getWrongArgumentCountEx(
			final Function func, final String expected, final int count) {
		return new IllegalArgumentException(
				String.format(
						"Wrong number of arguments provided (%s) to %s function, %s expected",
						count, func.getName().toUpperCase(), expected));
	}

	/**
	 * Return true if this Handler handles the function;
	 *
	 * @param func
	 * @return
	 * @throws SQLException
	 */
	abstract public FuncInfo handle(Function func, String alias)
			throws SQLException;

	protected FuncInfo handleExpr0(final Class<? extends ExprFunction0> clazz,
			final Function func, final int type, final String alias)
			throws SQLException {
		final ExpressionList l = func.getParameters();
		if (l != null) {
			throw getToManyArgumentEx(func, "zero");
		}
		try {
			final ExprFunction0 expr = clazz.newInstance();
			// stack.push(expr);
			// final ColumnName colName = tblName.getColumnName(func.getName());
			final ColumnName colName = tblName.getColumnName(alias);
			builder.registerFunction(colName, type);
			return new FuncInfo(expr, colName,
					Collections.<ExprColumn> emptySet());
		} catch (final InstantiationException e) {
			throw new IllegalStateException(e.getMessage(), e);
		} catch (final IllegalAccessException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	protected FuncInfo handleExpr1(final Class<? extends ExprFunction1> clazz,
			final Function func, final int type, final String alias)
			throws SQLException {
		final ExpressionList l = func.getParameters();
		if (l == null) {
			throw getNoArgumentEx(func, "one");
		}
		if (l.getExpressions().size() > 1) {
			throw getToManyArgumentEx(func, "one");
		}
		try {
			final Constructor<? extends ExprFunction1> c = clazz
					.getConstructor(Expr.class);
			final Expression expression = (Expression) l.getExpressions()
					.get(0);
			expression.accept(exprVisitor);
			final ExprFunction1 expr = c.newInstance(exprVisitor.getResult());

			// stack.push(expr);
			// final ColumnName colName = tblName.getColumnName(func.getName());
			final ColumnName colName = tblName.getColumnName(alias);
			builder.registerFunction(colName, type);
			return new FuncInfo(expr, colName, exprVisitor.getColumns());

			// builder.addVar(expr, colName);

		} catch (final NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		} catch (final InstantiationException e) {
			throw new IllegalStateException(e.getMessage(), e);
		} catch (final IllegalAccessException e) {
			throw new IllegalStateException(e.getMessage(), e);
		} catch (final InvocationTargetException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	/**
	 * Implements expression for a function and contains the ColumnInfo.
	 *
	 */
	public static class FuncInfo implements Expr {
		public ColumnName columnName;
		public Expr expr;
		public Set<ExprColumn> columns;

		@Override
		public boolean isSatisfied(final Binding binding,
				final FunctionEnv execCxt) {
			return expr.isSatisfied(binding, execCxt);
		}

		@Override
		public Set<Var> getVarsMentioned() {
			return expr.getVarsMentioned();
		}

		@Override
		public void varsMentioned(final Collection<Var> acc) {
			expr.varsMentioned(acc);
		}

		@Override
		public NodeValue eval(final Binding binding, final FunctionEnv env) {
			return expr.eval(binding, env);
		}

		@Override
		public Expr copySubstitute(final Binding binding) {
			return expr.copySubstitute(binding);
		}

		@Override
		public Expr applyNodeTransform(final NodeTransform transform) {
			return expr.applyNodeTransform(transform);
		}

		@Override
		public Expr deepCopy() {
			return expr.deepCopy();
		}

		@Override
		public boolean isVariable() {
			return expr.isVariable();
		}

		@Override
		public String getVarName() {
			return expr.getVarName();
		}

		@Override
		public ExprVar getExprVar() {
			return expr.getExprVar();
		}

		@Override
		public Var asVar() {
			return expr.asVar();
		}

		@Override
		public boolean isConstant() {
			return expr.isConstant();
		}

		@Override
		public NodeValue getConstant() {
			return expr.getConstant();
		}

		@Override
		public boolean isFunction() {
			return expr.isFunction();
		}

		@Override
		public ExprFunction getFunction() {
			return expr.getFunction();
		}

		@Override
		public void visit(final ExprVisitor visitor) {
			expr.visit(visitor);
		}

		public Set<ExprColumn> getColumns() {
			return columns;
		}

		public FuncInfo(final Expr expr, final ColumnName columnName,
				final Set<ExprColumn> columns) {
			this.expr = expr;
			this.columnName = columnName;
			this.columns = columns;
		}

	}
}
