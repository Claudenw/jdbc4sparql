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
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprVar;

import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
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

	private void applyOuterExpr( final Expr aExpr )
	{
		final Expr expr = aExpr;
		if (expr instanceof ExprFunction)
		{
			if (applyOuterExprSub(aExpr, aExpr))
			{
				return;
			}
		}
		queryBuilder.addFilter(expr);
	}

	private boolean applyOuterExprSub( final Expr aExpr, final Expr toApply )
	{
		final Expr expr = aExpr;
		if (expr instanceof ExprFunction)
		{
			for (final Expr subExpr : ((ExprFunction) expr).getArgs())
			{
				if (applyOuterExprSub(subExpr, toApply))
				{
					return true;
				}
			}
			return false;
		}
		if (expr instanceof ExprVar)
		{
			final Node n = ((ExprVar) expr).getAsNode();
			final SparqlQueryBuilder.RdfTableInfo sti = queryBuilder
					.getNodeTable(n);
			if ((sti != null) && sti.isOptional())
			{
				sti.addFilter(toApply);
				return true;
			}
			return false;
		}
		return false;
	}

	// take apart the join and figure out how to merge it.
	private void deparseJoin( final Join join ) 
	{
		if (join.isSimple())
		{
			final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
					queryBuilder);
			join.getRightItem().accept(fromVisitor);
		}
		else if (join.isOuter())
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
						queryBuilder, SparqlFromVisitor.OPTIONAL);
				join.getRightItem().accept(fromVisitor);

				if (join.getOnExpression() != null)
				{
					final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
							queryBuilder);
					join.getOnExpression().accept(expressionVisitor);
					applyOuterExpr(expressionVisitor.getResult());
				}
			}
		}
		else
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
							queryBuilder);
					join.getOnExpression().accept(expressionVisitor);
					queryBuilder.addFilter(expressionVisitor.getResult());
				}
				if (join.getUsingColumns() != null)
				{
					for (Object c : join.getUsingColumns())
					{
						queryBuilder.addUsing( ((Column)c).getColumnName() );
					}
				}
			}
		}

	}

	// process a limit
	private void deparseLimit( final Limit limit )
	{
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

	private void deparseOrderBy( final List orderByElements )
	{
		for (final Iterator iter = orderByElements.iterator(); iter.hasNext();)
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
		final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
				queryBuilder);
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
		final SparqlFromVisitor fromVisitor = new SparqlFromVisitor(
				queryBuilder);
		final SparqlExprVisitor expressionVisitor = new SparqlExprVisitor(
				queryBuilder);
		final SparqlSelectItemVisitor selectItemVisitor = new SparqlSelectItemVisitor(
				queryBuilder);

		// Process FROM to get table names loaded in the builder
		if (plainSelect.getFromItem() != null)
		{
			plainSelect.getFromItem().accept(fromVisitor);
		}

		if (plainSelect.getDistinct() != null)
		{
			queryBuilder.setDistinct();
			if (plainSelect.getDistinct().getOnSelectItems() != null)
			{
				throw new UnsupportedOperationException(
						"DISTINCT ON() is not supported");
			}
		}

		// process Joins to pick up new tables
		if (plainSelect.getJoins() != null)
		{
			for (final Iterator iter = plainSelect.getJoins().iterator(); iter
					.hasNext();)
			{
				final Join join = (Join) iter.next();
				deparseJoin(join);
			}
		}

		// process the select -- All tables must be identified before this.
		for (final Iterator iter = plainSelect.getSelectItems().iterator(); iter
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
			throw new UnsupportedOperationException("GROUP BY is not supported");
			// buffer.append(" GROUP BY ");
			// for (Iterator iter =
			// plainSelect.getGroupByColumnReferences().iterator();
			// iter.hasNext();) {
			// Expression columnReference = (Expression) iter.next();
			// columnReference.accept(expressionVisitor);
			// if (iter.hasNext()) {
			// buffer.append(", ");
			// }
			// }
		}

		if (plainSelect.getHaving() != null)
		{
			throw new UnsupportedOperationException("HAVING is not supported");
			// buffer.append(" HAVING ");
			// plainSelect.getHaving().accept(expressionVisitor);
		}

		if (plainSelect.getOrderByElements() != null)
		{
			deparseOrderBy(plainSelect.getOrderByElements());
		}

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

		if (limit != null)
		{
			deparseLimit(plainSelect.getLimit());
		}

	}

	@Override
	public void visit( final Union union )
	{
		throw new UnsupportedOperationException("UNION is not supported");
	}

}
