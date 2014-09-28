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

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;

import java.sql.SQLException;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.sparql.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

/**
 * A visitor that process the SQL select into the SparqlQueryBuilder.
 */
class SparqlSelectItemVisitor implements SelectItemVisitor
{
	// the query builder.
	private final SparqlQueryBuilder queryBuilder;
	private static Logger LOG = LoggerFactory
			.getLogger(SparqlSelectItemVisitor.class);

	/**
	 * Constructor
	 * 
	 * @param queryBuilder
	 *            The query builder.
	 */
	SparqlSelectItemVisitor( final SparqlQueryBuilder queryBuilder )
	{
		this.queryBuilder = queryBuilder;
	}

	@Override
	public void visit( final AllColumns allColumns )
	{
		SparqlSelectItemVisitor.LOG.debug("visit All Columns {}", allColumns);
		try
		{
			queryBuilder.setAllColumns();
		}
		catch (final SQLException e)
		{
			SparqlSelectItemVisitor.LOG.error(
					"Error visitin all columns: " + e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void visit( final AllTableColumns allTableColumns )
	{
		SparqlSelectItemVisitor.LOG.debug("visit All Table Columns {}",
				allTableColumns.toString());
		
		TableName name = null;
		if (allTableColumns.getTable().getAlias() != null)
		{
			name = TableName.getNameInstance(allTableColumns.getTable().getAlias());
		}
		else
		{
			name = new TableName(allTableColumns.getTable().getSchemaName(),
					allTableColumns.getTable().getName());
		}
		
		QueryTableInfo tableInfo = queryBuilder.getTable( name );
		queryBuilder.addTableColumns( tableInfo );
	}

	@Override
	public void visit( final SelectExpressionItem selectExpressionItem )
	{
		SparqlSelectItemVisitor.LOG.debug("visit Select {}",
				selectExpressionItem);
		final SparqlExprVisitor v = new SparqlExprVisitor(queryBuilder, SparqlQueryBuilder.OPTIONAL);
		selectExpressionItem.getExpression().accept(v);
		Expr expr = v.getResult();

		// handle explicit name mapping
		String exprAlias = null;
		if (selectExpressionItem.getAlias() != null)
		{
			exprAlias = NameUtils.convertDB2SPARQL(selectExpressionItem
					.getAlias());
		}
		else if (selectExpressionItem.getExpression() instanceof net.sf.jsqlparser.schema.Column)
		{
			exprAlias = NameUtils.convertDB2SPARQL(selectExpressionItem
					.getExpression().toString());
		}
		if (expr instanceof ExprAggregator)
		{
			ExprAggregator agg = (ExprAggregator) expr;
			if (exprAlias == null)
			{
				exprAlias = agg.getVar().getName();
			}
		}

		queryBuilder.addVar(expr, exprAlias);
	}

}