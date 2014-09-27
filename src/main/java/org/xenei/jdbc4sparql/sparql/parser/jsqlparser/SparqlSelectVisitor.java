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

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprVar;
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
import org.xenei.jdbc4sparql.iface.ColumnName;
import org.xenei.jdbc4sparql.iface.ItemName;
import org.xenei.jdbc4sparql.iface.TableName;
import org.xenei.jdbc4sparql.sparql.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

/**
 * Implementation of SelectVisitor and OrderByVisitor that merge the SQL
 * commands
 * into the SparqlQueryBuilder.
 */
public class SparqlSelectVisitor implements SelectVisitor, OrderByVisitor
{
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
	SparqlSelectVisitor( final SparqlQueryBuilder queryBuilder )
	{
		this.queryBuilder = queryBuilder;
	}

	private void applyOuterExpr( final Expr aExpr, final ItemName mapFrom,
			final ItemName mapTo )
	{
		SparqlSelectVisitor.LOG.debug("apply outer expr {}", aExpr);
		final Expr expr = aExpr;
		if (expr instanceof ExprFunction)
		{
			if (applyOuterExprSub(aExpr, aExpr, mapFrom, mapTo))
			{
				return;
			}
		}
		queryBuilder.addFilter(expr);
	}

	private boolean applyOuterExprSub( final Expr aExpr, final Expr toApply,
			final ItemName mapFrom, final ItemName mapTo )
	{
		SparqlSelectVisitor.LOG.debug("apply outer expr sub {} {}", aExpr,
				toApply);
		final Expr expr = aExpr;
		if (expr instanceof ExprFunction)
		{
			for (final Expr subExpr : ((ExprFunction) expr).getArgs())
			{
				if (applyOuterExprSub(subExpr, toApply, mapFrom, mapTo))
				{
					return true;
				}
			}
			return false;
		}
		if (expr instanceof ExprVar)
		{
			final Node n = ((ExprVar) expr).getAsNode();
			final QueryTableInfo tableInfo = queryBuilder.getNodeTable(n);
			if ((tableInfo != null) && (tableInfo.isOptional()))
			{
				tableInfo.addFilter(toApply);
				return true;
			}
			return false;
		}
		return false;
	}

	// take apart the join and figure out how to merge it.
	private void deparseJoin( final Join join, final TableName tableName) 
	{
		SparqlSelectVisitor.LOG.debug("deparse join {}", join);
		if (join.isSimple())
		{
			final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
					queryBuilder);
			join.getRightItem().accept(fromVisitor);
		}
		else if (join.isOuter())
		{
			deparseOuterJoin(join, tableName);
		}
		else {
			deparseInnerJoin(join, tableName);
		}
	}
	
	private void deparseOuterJoin(final Join join, final ItemName tableName)
	{
			final String fmt = "%s OUTER JOIN Is not supported";

			if (join.isRight())
			{
				throw new UnsupportedOperationException(String.format(fmt,
						"RIGHT"));
			}
			else if (join.isNatural())
			{ // this is one case we will not support as it is generally
				// considered bad.
				throw new UnsupportedOperationException(String.format(fmt,
						"NATURAL"));
			}
			else if (join.isFull())
			{
				throw new UnsupportedOperationException(String.format(fmt,
						"FULL"));
			}
			else
			{
				// handles left and not specified
				final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
						queryBuilder, SparqlQueryBuilder.OPTIONAL);
				join.getRightItem().accept(fromVisitor);

				if (join.getOnExpression() != null)
				{
					final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
							queryBuilder,SparqlQueryBuilder.REQUIRED);
					join.getOnExpression().accept(expressionVisitor);
					final ExpRewriter rewriter = new JoinRewriter(queryBuilder){
						
						@Override
						protected Node addAlias( final QueryColumnInfo columnInfo,
								final ColumnName alias )
						{
							final TableName tName = TableName
									.getNameInstance(columnInfo.getName());
							final QueryTableInfo tableInfo = queryBuilder.getTable(tName);
							tableInfo.setEquals( columnInfo,  alias );
							return queryBuilder.getColumn( alias ).getVar();
							//return tableInfo.addColumnToQuery(columnInfo.getColumn(), columnInfo.getName());
						}
					};
					rewriter.addMap(fromVisitor.getName(), tableName);
					expressionVisitor.getResult().visit(rewriter);
					applyOuterExpr(rewriter.getResult(), fromVisitor.getName(),
							tableName);
				}
			}
		}
	
	private void deparseInnerJoin(final Join join, final ItemName tableName)
	{
			// inner join
			// select * from table join othertable on table.id = othertable.fk
			final String fmt = "%s INNER JOIN Is not supported";
			if (join.isRight())
			{ // this should never happen anyway
				throw new UnsupportedOperationException(String.format(fmt,
						"RIGHT"));
			}
			else if (join.isNatural())
			{ // this is one case we will not support as it is generally
				// considered bad.
				throw new UnsupportedOperationException(String.format(fmt,
						"NATURAL"));
			}
			else if (join.isFull())
			{ // this should never happen anyway
				throw new UnsupportedOperationException(String.format(fmt,
						"FULL"));
			}
			else if (join.isLeft())
			{ // this should never happen anyway
				throw new UnsupportedOperationException(String.format(fmt,
						"LEFT"));
			}
			else
			{
				final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
						queryBuilder);
				join.getRightItem().accept(fromVisitor);
				if (join.getOnExpression() != null)
				{
					final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
							queryBuilder, SparqlQueryBuilder.REQUIRED);
					join.getOnExpression().accept(expressionVisitor);
					final ExpRewriter rewriter = new JoinRewriter(queryBuilder){
						@Override
						protected Node addAlias( final QueryColumnInfo columnInfo,
								final ColumnName alias )
						{
							final TableName tName = TableName
									.getNameInstance(columnInfo.getName());
							final QueryTableInfo tableInfo = queryBuilder.getTable(tName);
							return tableInfo.addColumnToQuery(columnInfo, alias);
						}
					};
					rewriter.addMap(fromVisitor.getName(), tableName);
					expressionVisitor.getResult().visit(rewriter);
					queryBuilder.addFilter(rewriter.getResult());
				}
				if (join.getUsingColumns() != null)
				{
					queryBuilder.setForceShortName(true);
					for (final Object c : join.getUsingColumns())
					{
						queryBuilder.addUsing(((Column) c).getColumnName());
					}
				}
			}
		}

	

	// process a limit
	private void deparseLimit( final Limit limit )
	{
		if (limit == null)
		{
			return;
		}
		SparqlSelectVisitor.LOG.debug("deparse limit {}", limit);
		// LIMIT n OFFSET skip
		if (limit.isOffsetJdbcParameter())
		{
			throw new UnsupportedOperationException(
					"LIMIT with OFFSET JDBC Parameter is not supported");
		}
		else if (limit.getOffset() != 0)
		{
			queryBuilder.setOffset(limit.getOffset());
		}
		if (limit.isRowCountJdbcParameter())
		{
			throw new UnsupportedOperationException(
					"LIMIT with JDBC Parameter is not supported");
			// buffer.append("?");
		}
		else if (limit.getRowCount() != 0)
		{
			queryBuilder.setLimit(limit.getRowCount());
		}
		else
		{
			throw new UnsupportedOperationException(
					"LIMIT with no parameter is not supported");
		}
	}
	
	private void deparseDistinct( Distinct distinct )
	{
		if (distinct == null)
		{
			return;
		}
		
		queryBuilder.setDistinct();
		if (distinct.getOnSelectItems() != null)
		{
			throw new UnsupportedOperationException(
					"DISTINCT ON() is not supported");
		}
	}

	private void deparseOrderBy( final List<?> orderByElements )
	{
		if (orderByElements == null)
		{
			return;
		}
		SparqlSelectVisitor.LOG.debug("deparse orderby {}", orderByElements);
		for (final Iterator<?> iter = orderByElements.iterator(); iter
				.hasNext();)
		{
			final OrderByElement orderByElement = (OrderByElement) iter.next();
			orderByElement.accept(this);
		}
	}

	/**
	 * Get the SPARQL query generated from the querybuilder.
	 * 
	 * @return The SPARQL query.
	 */
	public Query getQuery()
	{
		return queryBuilder.build();
	}

	@Override
	public void visit( final OrderByElement orderBy )
	{
		SparqlSelectVisitor.LOG.debug("visit orderby: {}", orderBy);
		final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
				queryBuilder, SparqlQueryBuilder.REQUIRED);
		orderBy.getExpression().accept(expressionVisitor);
		queryBuilder.addOrderBy(expressionVisitor.getResult(), orderBy.isAsc());
		if (!expressionVisitor.isEmpty())
		{
			throw new IllegalStateException(
					"Order By processing failed -- stack not empty");
		}
	}

	@Override
	public void visit( final PlainSelect plainSelect )
	{
		SparqlSelectVisitor.LOG.debug("visit plainSelect: {}", plainSelect);
		TableName lastTableName = null;
		final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
				queryBuilder, SparqlQueryBuilder.OPTIONAL);
		final SparqlSelectItemVisitor selectItemVisitor = new SparqlSelectItemVisitor(
				queryBuilder);

		// Process FROM to get table names loaded in the builder
		if (plainSelect.getFromItem() != null)
		{
			final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
					queryBuilder);
			plainSelect.getFromItem().accept(fromVisitor);
			lastTableName = fromVisitor.getName();
		}

		deparseDistinct( plainSelect.getDistinct() );

		// process Joins to pick up new tables
		if (plainSelect.getJoins() != null)
		{
			for (final Iterator<?> iter = plainSelect.getJoins().iterator(); iter
					.hasNext();)
			{
				final Join join = (Join) iter.next();
				deparseJoin(join, lastTableName);
			}
		}

		// add required columns -- All tables must be identified before this. 
		queryBuilder.addRequiredColumns();
		
		// process the select 
		for (final Iterator<?> iter = plainSelect.getSelectItems().iterator(); iter
				.hasNext();)
		{
			final SelectItem selectItem = (SelectItem) iter.next();
			selectItem.accept(selectItemVisitor);
		}

		// process the where to add filters.
		if (plainSelect.getWhere() != null)
		{
			plainSelect.getWhere().accept(expressionVisitor);
			queryBuilder.addFilter(expressionVisitor.getResult());
		}

		if (plainSelect.getGroupByColumnReferences() != null)
		{
			SparqlExprVisitor visitor = new SparqlExprVisitor(queryBuilder, SparqlQueryBuilder.REQUIRED);
			for (final Iterator<?> iter = plainSelect.getGroupByColumnReferences().iterator(); iter
					.hasNext();)
			{
				Expression e = (Expression) iter.next();
				e.accept(visitor);
				queryBuilder.addGroupBy( visitor.getResult() );
			}
		}

		if (plainSelect.getHaving() != null)
		{
			SparqlExprVisitor visitor = new SparqlExprVisitor(queryBuilder, SparqlQueryBuilder.REQUIRED);
			plainSelect.getHaving().accept( visitor );
			queryBuilder.setHaving( visitor.getResult() );
		}
		
		deparseOrderBy(plainSelect.getOrderByElements());
		
		// TOP is implements in SPARQL as LIMIT

		final Top top = plainSelect.getTop();
		Limit limit = plainSelect.getLimit();
		if ((top != null) && (limit != null))
		{
			throw new IllegalStateException(
					"Top and Limit may not both be specified");
		}
		if (top != null)
		{
			if (top.isRowCountJdbcParameter())
			{
				throw new UnsupportedOperationException(
						"TOP with JDBC Parameter is not supported");
			}
			limit = new Limit();
			limit.setRowCount(top.getRowCount());
		}

		deparseLimit(plainSelect.getLimit());

	}

	@Override
	public void visit( final Union union )
	{
		// TODO implement ALL
		if (union.isDistinct())
		{
			queryBuilder.setDistinct();
		}
		List<SparqlQueryBuilder> unionBuilders = new ArrayList<SparqlQueryBuilder>();
		for( Iterator<?> iter=union.getPlainSelects().iterator(); iter.hasNext();)
		{
			PlainSelect ps = (PlainSelect) iter.next();
			SparqlQueryBuilder sqb = new SparqlQueryBuilder( queryBuilder );
			ps.accept( new SparqlSelectVisitor( sqb ));
			unionBuilders.add(sqb);

		}
		queryBuilder.addUnion(unionBuilders);
		deparseOrderBy(union.getOrderByElements());
		deparseLimit( union.getLimit() );
		throw new UnsupportedOperationException("UNION is not supported");
	}

	private abstract static class JoinRewriter extends ExpRewriter {
		JoinRewriter( SparqlQueryBuilder queryBuilder){
			super( queryBuilder );
		}
		
		protected abstract Node addAlias( final QueryColumnInfo columnInfo,
				final ColumnName alias );
		
		@Override
		public void visit( final ExprFunction2 func )
		{
			if (func instanceof E_Equals)
			{
				final E_Equals eq = (E_Equals) func;
				if ((eq.getArg1() instanceof ExprVar)
						&& (eq.getArg2() instanceof ExprVar))
				{
					final QueryColumnInfo ci1 = queryBuilder
							.getNodeColumn(((ExprVar) eq.getArg1()).getAsNode());
					if (ci1 != null)
					{
						final QueryColumnInfo ci2 = queryBuilder
								.getNodeColumn(((ExprVar) eq.getArg2()).getAsNode());
						if (ci2 != null)
						{
							final ColumnName ci1a = isMapped(ci1);
							final ColumnName ci2a = isMapped(ci2);
							if (((ci1a != null) && (ci2a == null))
									|| ((ci1a == null) && (ci2a != null)))
							{
								Node n = null;
								// equals and one of the columns is aliased.
								if (ci1a != null)
								{
									// first one is aliased
									n = addAlias(ci1, ci1a);
								}
								else
								{
									n = addAlias(ci2, ci2a);
								}
								stack.push(new E_Bound(new ExprVar(n)));
								return;
							}
						}
					}
				}
			}

			pushArgs(func);
			stack.push(func.copy(stack.pop(), stack.pop()));
		}
	};
}
