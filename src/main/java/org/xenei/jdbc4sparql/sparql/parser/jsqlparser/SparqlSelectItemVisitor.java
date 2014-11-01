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
import com.hp.hpl.jena.sparql.expr.ExprNode;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ColumnName;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.virtual.VirtualCatalog;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.QueryColumnInfo;
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
			name = TableName.getNameInstance(allTableColumns.getTable()
					.getAlias());
		}
		else
		{
			name = new TableName(allTableColumns.getTable().getSchemaName(),
					allTableColumns.getTable().getName());
		}

		final QueryTableInfo tableInfo = queryBuilder.getTable(name);
		queryBuilder.addTableColumns(tableInfo);
	}

	@Override
	public void visit( final SelectExpressionItem selectExpressionItem )
	{
		SparqlSelectItemVisitor.LOG.debug("visit Select {}",
				selectExpressionItem);
		final SparqlExprVisitor v = new SparqlExprVisitor(queryBuilder,
				SparqlQueryBuilder.OPTIONAL);
		selectExpressionItem.getExpression().accept(v);
		final Expr expr = v.getResult();

		final AliasBuilder aliasBuilder = new AliasBuilder( selectExpressionItem);
		expr.visit(aliasBuilder);
		// handle explicit name mapping
//		String exprAlias = aliasBuilder.getAlias();
//		if (selectExpressionItem.getAlias() != null)
//		{
//			final ColumnName columnName = ColumnName
//					.getNameInstance(selectExpressionItem.getAlias());
//
//			exprAlias = NameUtils.convertDB2SPARQL(selectExpressionItem
//					.getAlias());
//			queryBuilder.addAlias(expr, columnName);
//
//		}
//		else {
//			
//		 if (selectExpressionItem.getExpression() instanceof net.sf.jsqlparser.schema.Column)
//		{
//			exprAlias = NameUtils.convertDB2SPARQL(selectExpressionItem
//					.getExpression().toString());
//		}
//		 else if (expr instanceof ExprAggregator)
//		{
//			final ExprAggregator agg = (ExprAggregator) expr;
//			exprAlias = agg.getVar().getName();
//		}
//		 else if (selectExpressionItem.getExpression() instanceof Function)
//		{
//			exprAlias = ((Function)selectExpressionItem.getExpression()).getName();
//			final ColumnName columnName = ColumnName
//					.getNameInstance(selectExpressionItem.getAlias());
//
//			exprAlias = NameUtils.convertDB2SPARQL(selectExpressionItem
//					.getAlias());
//			queryBuilder.addAlias(expr, columnName);
//
//		}
//		}
//		queryBuilder.addVar(expr, exprAlias);
	}

	private class AliasBuilder implements ExprVisitor, ExpressionVisitor {
		private SelectExpressionItem selectExpressionItem;
		private ColumnName alias;	
		
		private AliasBuilder( SelectExpressionItem selectExpressionItem) {
			this.selectExpressionItem=selectExpressionItem;
			String origAlias = selectExpressionItem.getAlias();
			if (origAlias != null)
			{
				alias = ColumnName.getNameInstance(origAlias);
			}
		}

		public String getAlias()
		{
			return alias.getSPARQLName();
		}
		
		private void setAlias(ExprNode node) 
		{
			if (alias == null)
			{
				if (node.isFunction())
				{
					ExprFunction func =  node.getFunction();
					alias = ColumnName.getNameInstance(func.getFunctionSymbol().getSymbol());
					// FIXME
					QueryColumnInfo columnInfo = queryBuilder.registerFunctionColumn( alias, java.sql.Types.INTEGER);
				} else if (node.isVariable()) {
					try {
						queryBuilder.addVar(ColumnName.getNameInstance( node.asVar().getName()));
					} catch (SQLException e) {
						throw new IllegalStateException( e.getMessage(), e );
					}
				} else if (node.isConstant()) {
					selectExpressionItem.getExpression().accept(this);
				} else if (node.isGraphPattern()) {
					alias = ColumnName.getNameInstance(node.getGraphPattern().getName());	
				}
			}
			else
			{
				queryBuilder.addVar(node, alias);
			}
		}
		
		@Override
		public void visit( ExprFunction0 func )
		{
			setAlias( func );
		}

		@Override
		public void visit( ExprFunction1 func )
		{
			setAlias( func );
		}

		@Override
		public void visit( ExprFunction2 func )
		{
			setAlias( func );
		}

		@Override
		public void visit( ExprFunction3 func )
		{
			setAlias( func );
		}

		@Override
		public void visit( ExprFunctionN func )
		{
			setAlias( func );
		}

		@Override
		public void visit( ExprFunctionOp funcOp )
		{
			setAlias( funcOp );
		}

		@Override
		public void visit( NodeValue nv )
		{
			setAlias( nv );
		}

		@Override
		public void visit( ExprVar nv )
		{
			setAlias( nv );
		}

		@Override
		public void visit( ExprAggregator eAgg )
		{
			alias = ColumnName.getNameInstance(eAgg.getVar().getName());
			// make sure the function column is in place
			queryBuilder.registerFunctionColumn( alias, java.sql.Types.NUMERIC);
			queryBuilder.addVar(eAgg, alias);
		}

		@Override
		public void finishVisit()
		{		
			// do nothing
		}

		@Override
		public void startVisit()
		{
			// do nothing
		}

		@Override
		public void visit( NullValue nullValue )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Function function )
		{
			alias = ColumnName.getNameInstance(function.getName());	
		}

		@Override
		public void visit( InverseExpression inverseExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( JdbcParameter jdbcParameter )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( DoubleValue doubleValue )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( LongValue longValue )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( DateValue dateValue )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( TimeValue timeValue )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( TimestampValue timestampValue )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Parenthesis parenthesis )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( StringValue stringValue )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Addition addition )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Division division )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Multiplication multiplication )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Subtraction subtraction )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( AndExpression andExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( OrExpression orExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Between between )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( EqualsTo equalsTo )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( GreaterThan greaterThan )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( GreaterThanEquals greaterThanEquals )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( InExpression inExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( IsNullExpression isNullExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( LikeExpression likeExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( MinorThan minorThan )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( MinorThanEquals minorThanEquals )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( NotEqualsTo notEqualsTo )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Column tableColumn )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( SubSelect subSelect )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( CaseExpression caseExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( WhenClause whenClause )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( ExistsExpression existsExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( AllComparisonExpression allComparisonExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( AnyComparisonExpression anyComparisonExpression )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Concat concat )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( Matches matches )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( BitwiseAnd bitwiseAnd )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( BitwiseOr bitwiseOr )
		{
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit( BitwiseXor bitwiseXor )
		{
			// TODO Auto-generated method stub
			
		}
		
	}
}