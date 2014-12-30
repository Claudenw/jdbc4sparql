package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

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
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.AliasInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfoFactory;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;

public abstract class AbstractFunctionHandler {
	protected SparqlQueryBuilder builder;
	protected final SparqlExprVisitor exprVisitor;
	protected final TableName tblName;

	public AbstractFunctionHandler(final SparqlQueryBuilder builder) {
		this.builder = builder;
		this.exprVisitor = new SparqlExprVisitor(builder,
				SparqlQueryBuilder.REQUIRED, false);
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
	abstract public Expr handle(Function func, AliasInfo alias)
			throws SQLException;

	protected Expr handleExpr0(final Class<? extends ExprFunction0> clazz,
			final Function func, final int type, final AliasInfo alias)
					throws SQLException {
		final ExpressionList l = func.getParameters();
		if (l != null) {
			throw getToManyArgumentEx(func, "zero");
		}
		try {
			final ExprFunction0 expr = clazz.newInstance();
			// stack.push(expr);
			// final ColumnName colName = tblName.getColumnName(func.getName());
			final ColumnName colName = tblName.getColumnName(alias.getAlias());
			builder.registerFunction(colName, type);
			return ExprInfoFactory.getInstance(expr, colName);
		} catch (final InstantiationException e) {
			throw new IllegalStateException(e.getMessage(), e);
		} catch (final IllegalAccessException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	protected Expr handleExpr1(final Class<? extends ExprFunction1> clazz,
			final Function func, final int type, final AliasInfo alias)
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
			final ColumnName colName = tblName.getColumnName(alias.getAlias());
			builder.registerFunction(colName, type);
			return ExprInfoFactory.getInstance(expr, exprVisitor.getColumns(),
					colName);

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

}
