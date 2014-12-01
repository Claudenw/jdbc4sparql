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

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;

import java.sql.SQLException;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

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
		if (LOG.isDebugEnabled())
			SparqlSelectItemVisitor.LOG.debug("visit All Columns {}", allColumns);
		try {
			queryBuilder.setAllColumns();
		} catch (final SQLException e) {
			SparqlSelectItemVisitor.LOG.error(
					"Error visitin all columns: " + e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void visit(final AllTableColumns allTableColumns) {
		if (LOG.isDebugEnabled())
			SparqlSelectItemVisitor.LOG.debug("visit All Table Columns {}",
				allTableColumns.toString());

		TableName name = null;
		if (allTableColumns.getTable().getAlias() != null) {
			String defaultCatalog = queryBuilder.getCatalogName();
			String defaultSchema = queryBuilder.getDefaultSchema().getName()
					.getShortName();
			name = TableName.getNameInstance(defaultCatalog, defaultSchema,
					allTableColumns.getTable().getAlias());
		} else {
			name = new TableName(queryBuilder.getCatalogName(), allTableColumns
					.getTable().getSchemaName(), allTableColumns.getTable()
					.getName());
		}

		final QueryTableInfo tableInfo = queryBuilder.getTable(name);
		queryBuilder.addTableColumns(tableInfo);
	}

	@Override
	public void visit(final SelectExpressionItem selectExpressionItem) {
		if (LOG.isDebugEnabled())
			SparqlSelectItemVisitor.LOG.debug("visit Select {}",
				selectExpressionItem);
		final SparqlExprVisitor v = new SparqlExprVisitor(queryBuilder,
				SparqlQueryBuilder.OPTIONAL);
		selectExpressionItem.getExpression().accept(v);
		final Expr expr = v.getResult();

		final AliasBuilder aliasBuilder = new AliasBuilder(
				selectExpressionItem, v.getAlias(), v.getColumnDef());
		expr.visit(aliasBuilder);
	}

	private class AliasBuilder implements ExprVisitor {
		private String origAlias;
		private ColumnDef backupDef;

		private AliasBuilder(SelectExpressionItem selectExpressionItem,
				String backupAlias, ColumnDef backupDef) {
			this.origAlias = StringUtils.defaultIfBlank(
					selectExpressionItem.getAlias(), backupAlias);
			this.backupDef = backupDef;
		}

		private void visit(ExprFunction func) {
			ColumnName alias = ColumnName.getNameInstance(VirtualCatalog.NAME,
					VirtualSchema.NAME, VirtualTable.NAME, StringUtils
							.defaultString(origAlias, func.getFunctionSymbol()
									.getSymbol()));

			queryBuilder.registerFunctionColumn(alias, backupDef.getType());
		}

		@Override
		public void visit(ExprFunction0 func) {
			visit((ExprFunction) func);
		}

		@Override
		public void visit(ExprFunction1 func) {
			visit((ExprFunction) func);
		}

		@Override
		public void visit(ExprFunction2 func) {
			visit((ExprFunction) func);
		}

		@Override
		public void visit(ExprFunction3 func) {
			visit((ExprFunction) func);
		}

		@Override
		public void visit(ExprFunctionN func) {
			visit((ExprFunction) func);
		}

		@Override
		public void visit(ExprFunctionOp funcOp) {
			throw new IllegalArgumentException("Function alias not supported");
		}

		@Override
		public void visit(NodeValue nv) {

			if (origAlias == null) {
				throw new IllegalArgumentException(
						"Constant without alias not supported");
			} else {
				ColumnName alias = ColumnName.getNameInstance(
						VirtualCatalog.NAME, VirtualSchema.NAME,
						VirtualTable.NAME, origAlias);
				queryBuilder.addVar(nv, alias);
			}
		}

		@Override
		public void visit(ExprVar nv) {
			ColumnName cName = null;
			if (nv instanceof ExprColumn) {
				QueryColumnInfo columnInfo = ((ExprColumn) nv).getColumnInfo();
				cName = columnInfo.getName();

			} else {
				String catalogName = queryBuilder.getCatalogName();
				String schemaName = queryBuilder.getDefaultSchemaName();
				String tableName = queryBuilder.getDefaultTableName();
				cName = ColumnName.getNameInstance(catalogName, schemaName,
						tableName, nv.asVar().getName());
			}
			if (origAlias == null) {
				try {
					queryBuilder.addVar(cName);
				} catch (SQLException e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}

			} else {
				ColumnName alias = ColumnName.getNameInstance(cName, origAlias);
				try {
					queryBuilder.addAlias(cName, alias);
					queryBuilder.addVar(nv, alias);
				} catch (SQLException e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			}

		}

		@Override
		public void visit(ExprAggregator eAgg) {
			ColumnName alias = ColumnName.getNameInstance(VirtualCatalog.NAME,
					VirtualSchema.NAME, VirtualTable.NAME, StringUtils
							.defaultString(origAlias, eAgg.getVar().getName()));

			// make sure the function column is in place
			queryBuilder.registerFunctionColumn(alias, java.sql.Types.NUMERIC);
			queryBuilder.addVar(eAgg, alias);
		}

		@Override
		public void finishVisit() {
			// do nothing
		}

		@Override
		public void startVisit() {
			// do nothing
		}

	}
}