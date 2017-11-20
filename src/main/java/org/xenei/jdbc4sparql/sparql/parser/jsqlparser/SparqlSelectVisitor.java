/*
 * This file is part of jdbc4sparql jsqlparser implementation.
 *
 * jdbc4sparql jsqlparser implementation is free software: you can redistribute
 * it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * jdbc4sparql jsqlparser implementation is distributed in the hope that it will
 * be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with jdbc4sparql jsqlparser implementation. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.sql.SQLDataException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.select.Union;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_Bound;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprVar;

/**
 * Implementation of SelectVisitor and OrderByVisitor that merge the SQL
 * commands into the SparqlQueryBuilder.
 */
public class SparqlSelectVisitor implements SelectVisitor, OrderByVisitor {
	// the query builder
	private final SparqlQueryBuilder queryBuilder;

	private static Logger LOG = LoggerFactory
			.getLogger(SparqlSelectVisitor.class);

	/**
	 * Constructor
	 *
	 * @param queryBuilder
	 *            The builder to user.
	 */
	SparqlSelectVisitor(final SparqlQueryBuilder queryBuilder) {
		this.queryBuilder = queryBuilder;
	}

	private void applyOuterExpr(final Expr aExpr, final ItemName mapFrom,
			final ItemName mapTo) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectVisitor.LOG.debug("apply outer expr {}", aExpr);
		}
		if (aExpr instanceof ExprFunction) {
			applyOuterExprSub(aExpr, aExpr, mapFrom, mapTo);
		}
	}

	private void applyOuterExprSub(final Expr aExpr, final Expr toApply,
			final ItemName mapFrom, final ItemName mapTo) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectVisitor.LOG.debug("apply outer expr sub {} {}", aExpr,
					toApply);
		}
		final Expr expr = aExpr;
		if (expr instanceof ExprFunction) {
			for (final Expr subExpr : ((ExprFunction) expr).getArgs()) {
				applyOuterExprSub(subExpr, toApply, mapFrom, mapTo);
			}
		} else
		if (expr instanceof ExprVar) {
			QueryColumnInfo columnInfo = null;
			if (expr instanceof ExprColumn)
			{
				columnInfo = ((ExprColumn)expr).getColumnInfo();
			}
			else
			{
				columnInfo = queryBuilder.getColumn(((ExprVar) expr).asVar());
			}
			TableName tName = columnInfo.getName().getTableName();
			final QueryTableInfo tableInfo = queryBuilder.getTable(tName);
			tableInfo.addDataFilter(columnInfo);
			if ((tableInfo != null) && (tableInfo.isOptional())) {
				tableInfo.addJoinElement(toApply);
			}
		}
	}

	private void deparseDistinct(final Distinct distinct) {
		if (distinct == null) {
			return;
		}

		queryBuilder.setDistinct();
		if (distinct.getOnSelectItems() != null) {
			throw new UnsupportedOperationException(
					"DISTINCT ON() is not supported");
		}
	}

	private void deparseInnerJoin(final Join join, final ItemName tableName) {
		// inner join
		// select * from table join othertable on table.id = othertable.fk
		final String fmt = "%s INNER JOIN Is not supported";
		if (join.isRight()) { // this should never happen anyway
			throw new UnsupportedOperationException(String.format(fmt, "RIGHT"));
		}
		else if (join.isNatural()) { // this is one case we will not support
			// as it is generally
			// considered bad.
			throw new UnsupportedOperationException(String.format(fmt,
					"NATURAL"));
		}
		else if (join.isFull()) { // this should never happen anyway
			throw new UnsupportedOperationException(String.format(fmt, "FULL"));
		}
		else if (join.isLeft()) { // this should never happen anyway
			throw new UnsupportedOperationException(String.format(fmt, "LEFT"));
		}
		else {
			final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
					queryBuilder);
			join.getRightItem().accept(fromVisitor);
			if (join.getOnExpression() != null) {
				final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
						queryBuilder, SparqlQueryBuilder.REQUIRED, false);
				join.getOnExpression().accept(expressionVisitor);
				queryBuilder.addFilter(expressionVisitor.getResult());
				for (final ExprColumn exprCol : expressionVisitor.getColumns()) {
					final QueryColumnInfo paramColumnInfo = exprCol
							.getColumnInfo();
					queryBuilder.getTable(
							paramColumnInfo.getName().getTableName())
							.addDataFilter(paramColumnInfo);

				}
			}
			if (join.getUsingColumns() != null) {
				for (final Object c : join.getUsingColumns()) {
					queryBuilder.addUsing(((Column) c).getColumnName());
				}
			}
		}
	}

	// take apart the join and figure out how to merge it.
	private void deparseJoin(final Join join, final TableName tableName) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectVisitor.LOG.debug("deparse join {}", join);
		}
		if (join.isSimple()) {
			final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
					queryBuilder);
			join.getRightItem().accept(fromVisitor);
		}
		else if (join.isOuter()) {
			deparseOuterJoin(join, tableName);
		}
		else {
			deparseInnerJoin(join, tableName);
		}
	}

	// process a limit
	private void deparseLimit(final Limit limit) {
		if (limit == null) {
			return;
		}
		if (LOG.isDebugEnabled()) {
			SparqlSelectVisitor.LOG.debug("deparse limit {}", limit);
		}
		// LIMIT n OFFSET skip
		if (limit.isOffsetJdbcParameter()) {
			throw new UnsupportedOperationException(
					"LIMIT with OFFSET JDBC Parameter is not supported");
		}
		else if (limit.getOffset() != 0) {
			queryBuilder.setOffset(limit.getOffset());
		}
		if (limit.isRowCountJdbcParameter()) {
			throw new UnsupportedOperationException(
					"LIMIT with JDBC Parameter is not supported");
			// buffer.append("?");
		}
		else if (limit.getRowCount() != 0) {
			queryBuilder.setLimit(limit.getRowCount());
		}
		else {
			throw new UnsupportedOperationException(
					"LIMIT with no parameter is not supported");
		}
	}

	private void deparseOrderBy(final List<?> orderByElements) {
		if (orderByElements == null) {
			return;
		}
		if (LOG.isDebugEnabled()) {
			SparqlSelectVisitor.LOG
					.debug("deparse orderby {}", orderByElements);
		}
		for (final Object name : orderByElements) {
			final OrderByElement orderByElement = (OrderByElement) name;
			orderByElement.accept(this);
		}
	}

	private void deparseOuterJoin(final Join join, final ItemName tableName) {
		final String fmt = "%s OUTER JOIN Is not supported";

		if (join.isRight()) {
			throw new UnsupportedOperationException(String.format(fmt, "RIGHT"));
		}
		else if (join.isNatural()) { // this is one case we will not support
			// as it is generally
			// considered bad.
			throw new UnsupportedOperationException(String.format(fmt,
					"NATURAL"));
		}
		else if (join.isFull()) {
			throw new UnsupportedOperationException(String.format(fmt, "FULL"));
		}
		else {
			// handles left and not specified
			final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
					queryBuilder, SparqlQueryBuilder.OPTIONAL);
			join.getRightItem().accept(fromVisitor);

			if (join.getOnExpression() != null) {
				
				final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
						queryBuilder, SparqlQueryBuilder.REQUIRED, false);
				join.getOnExpression().accept(expressionVisitor);
				applyOuterExpr(expressionVisitor.getResult(), fromVisitor.getName(),
						tableName);

			}
			if (join.getUsingColumns() != null) {
				for (final Object c : join.getUsingColumns()) {
					queryBuilder.addUsing(((Column) c).getColumnName());
				}
			}
		}
	}

	/**
	 * Get the SPARQL query generated from the querybuilder.
	 *
	 * @return The SPARQL query.
	 * @throws SQLDataException
	 */
	public Query getQuery() throws SQLDataException {
		return queryBuilder.build();
	}

	@Override
	public void visit(final OrderByElement orderBy) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectVisitor.LOG.debug("visit orderby: {}", orderBy);
		}
		final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
				queryBuilder, SparqlQueryBuilder.REQUIRED, false);
		orderBy.getExpression().accept(expressionVisitor);
		queryBuilder.addOrderBy(expressionVisitor.getResult(), orderBy.isAsc());
		if (!expressionVisitor.isEmpty()) {
			throw new IllegalStateException(
					"Order By processing failed -- stack not empty");
		}
	}

	@Override
	public void visit(final PlainSelect plainSelect) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectVisitor.LOG.debug("visit plainSelect: {}", plainSelect);
		}
		TableName lastTableName = null;

		final SparqlSelectItemVisitor selectItemVisitor = new SparqlSelectItemVisitor(
				queryBuilder);

		// Process FROM to get table names loaded in the builder
		if (plainSelect.getFromItem() != null) {
			final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
					queryBuilder);
			plainSelect.getFromItem().accept(fromVisitor);
			lastTableName = fromVisitor.getName();
		}

		deparseDistinct(plainSelect.getDistinct());

		// process Joins to pick up new tables
		if (plainSelect.getJoins() != null) {
			for (final Iterator<?> iter = plainSelect.getJoins().iterator(); iter
					.hasNext();) {
				final Join join = (Join) iter.next();
				deparseJoin(join, lastTableName);
			}
		}

		// add required columns -- All tables must be identified before this.
		try {
			queryBuilder.addDefinedColumns();
		} catch (final SQLDataException e1) {
			throw new IllegalStateException(e1.getMessage(), e1);
		}
		queryBuilder.setSegmentCount();

		// process the select
		for (final Iterator<?> iter = plainSelect.getSelectItems().iterator(); iter
				.hasNext();) {
			final SelectItem selectItem = (SelectItem) iter.next();
			selectItem.accept(selectItemVisitor);
		}

		// process the where to add filters.
		if (plainSelect.getWhere() != null) {
			final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
					queryBuilder, SparqlQueryBuilder.OPTIONAL, false);
			plainSelect.getWhere().accept(expressionVisitor);
			final Expr expr = expressionVisitor.getResult();
			queryBuilder.addFilter(expr);
			for (final ExprColumn exprCol : expressionVisitor.getColumns()) {
				final QueryColumnInfo paramColumnInfo = exprCol.getColumnInfo();
				queryBuilder.getTable(paramColumnInfo.getName().getTableName())
						.addDataFilter(paramColumnInfo);
			}
		}

		if (plainSelect.getGroupByColumnReferences() != null) {
			final SparqlExprVisitor visitor = new SparqlExprVisitor(
					queryBuilder, SparqlQueryBuilder.REQUIRED, false);
			for (final Iterator<?> iter = plainSelect
					.getGroupByColumnReferences().iterator(); iter.hasNext();) {
				final Expression e = (Expression) iter.next();
				e.accept(visitor);
				queryBuilder.addGroupBy(visitor.getResult());
			}
		}

		if (plainSelect.getHaving() != null) {
			final SparqlExprVisitor visitor = new SparqlExprVisitor(
					queryBuilder, SparqlQueryBuilder.REQUIRED, false);
			plainSelect.getHaving().accept(visitor);
			queryBuilder.setHaving(visitor.getResult());
		}

		deparseOrderBy(plainSelect.getOrderByElements());

		// TOP is implements in SPARQL as LIMIT
		final Top top = plainSelect.getTop();
		Limit limit = plainSelect.getLimit();
		if ((top != null) && (limit != null)) {
			throw new IllegalStateException(
					"Top and Limit may not both be specified");
		}
		if (top != null) {
			if (top.isRowCountJdbcParameter()) {
				throw new UnsupportedOperationException(
						"TOP with JDBC Parameter is not supported");
			}
			limit = new Limit();
			limit.setRowCount(top.getRowCount());
		}

		deparseLimit(plainSelect.getLimit());
	}

	@Override
	public void visit(final Union union) {
		// TODO implement ALL
		if (union.isDistinct()) {
			queryBuilder.setDistinct();
		}
		final List<SparqlQueryBuilder> unionBuilders = new ArrayList<SparqlQueryBuilder>();
		for (final Iterator<?> iter = union.getPlainSelects().iterator(); iter
				.hasNext();) {
			final PlainSelect ps = (PlainSelect) iter.next();
			final SparqlQueryBuilder sqb = new SparqlQueryBuilder(queryBuilder);
			ps.accept(new SparqlSelectVisitor(sqb));
			unionBuilders.add(sqb);

		}

		try {
			queryBuilder.addUnion(unionBuilders);
		} catch (final SQLDataException e) {
			throw new IllegalStateException(e);
		}

		deparseOrderBy(union.getOrderByElements());
		deparseLimit(union.getLimit());
		throw new UnsupportedOperationException("UNION is not supported");
	};
}
