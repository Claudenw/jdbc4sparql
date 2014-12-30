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

import java.sql.SQLException;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfo;

import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * A visitor that process the SQL select into the SparqlQueryBuilder.
 */
class SparqlSelectItemVisitor implements SelectItemVisitor {
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
	SparqlSelectItemVisitor(final SparqlQueryBuilder queryBuilder) {
		this.queryBuilder = queryBuilder;
	}

	@Override
	public void visit(final AllColumns allColumns) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectItemVisitor.LOG.debug("visit All Columns {}",
					allColumns);
		}
		try {
			queryBuilder.setAllColumns();
		} catch (final SQLException e) {
			SparqlSelectItemVisitor.LOG.error("Error visiting all columns: "
					+ e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void visit(final AllTableColumns allTableColumns) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectItemVisitor.LOG.debug("visit All Table Columns {}",
					allTableColumns.toString());
		}
		TableName name = null;
		final String defaultCatalog = queryBuilder.getCatalogName();
		final String defaultSchema = queryBuilder.getDefaultSchema().getName()
				.getShortName();
		if (allTableColumns.getTable().getAlias() != null) {
			name = TableName.getNameInstance(defaultCatalog, defaultSchema,
					allTableColumns.getTable().getAlias());
		}
		else {
			name = new TableName(defaultCatalog, defaultSchema, allTableColumns
					.getTable().getName());
		}

		final QueryTableInfo tableInfo = queryBuilder.getTable(name);
		queryBuilder.addTableColumns(tableInfo);
	}

	@Override
	public void visit(final SelectExpressionItem selectExpressionItem) {
		if (LOG.isDebugEnabled()) {
			SparqlSelectItemVisitor.LOG.debug("visit Select {}",
					selectExpressionItem);
		}
		final String alias = selectExpressionItem.getAlias();
		// alias required if not a column
		boolean aliasRequired = true;
		if (selectExpressionItem.getExpression() instanceof Column) {
			// alias is required if a column and an alias is provided.
			aliasRequired = alias != null;
		}

		final SparqlExprVisitor v = new SparqlExprVisitor(queryBuilder,
				SparqlQueryBuilder.OPTIONAL, aliasRequired, alias);
		selectExpressionItem.getExpression().accept(v);

		Expr expr = v.getResult();
		if (expr instanceof ExprInfo) {
			final ExprInfo exprInfo = (ExprInfo) expr;
			queryBuilder.addVar(exprInfo.getExpr(), exprInfo.getName()
					.getSPARQLName());
			for (final ExprColumn column : exprInfo.getColumns()) {
				final QueryColumnInfo paramColumnInfo = column.getColumnInfo();
				queryBuilder.getTable(paramColumnInfo.getName().getTableName())
						.addDataFilter(paramColumnInfo);
			}
			expr = exprInfo.getExpr();
		}
		else if (expr instanceof ExprColumn) {
			final ExprColumn exprColumn = (ExprColumn) expr;
			final ColumnName columnName = exprColumn.getColumnInfo().getName();
			queryBuilder.getTable(columnName.getTableName()).addDataFilter(
					exprColumn.getColumnInfo());
			if (exprColumn.getVarName().equals(columnName.getSPARQLName())) {
				try {
					queryBuilder.addVar(columnName);
				} catch (final SQLException e) {
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
			else {
				queryBuilder.addVar(expr, columnName);
			}

		}
		else {
			if (expr.getVarName() == null) {
				throw new IllegalArgumentException(String.format(
						"function (%s) must have alias", expr));
			}
			final ColumnName columnName = ColumnName.getNameInstance(
					VirtualCatalog.NAME, VirtualSchema.NAME, VirtualTable.NAME,
					expr.getVarName());
			queryBuilder.addVar(expr, columnName);
		}

	}
}