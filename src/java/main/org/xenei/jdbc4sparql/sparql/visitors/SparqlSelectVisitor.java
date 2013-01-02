package org.xenei.jdbc4sparql.sparql.visitors;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;


import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.select.Union;

public class SparqlSelectVisitor implements SelectVisitor, OrderByVisitor
{
	private SparqlQueryBuilder queryBuilder;
	
	public SparqlSelectVisitor( SparqlQueryBuilder queryBuilder )
	{
		this.queryBuilder = queryBuilder;
	}
	
	public Query getQuery()
	{
		return queryBuilder.build();
	}
	
	@Override
	public void visit( PlainSelect plainSelect )
	{
		SparqlFromVisitor fromVisitor = new SparqlFromVisitor( queryBuilder );
		SparqlExprVisitor expressionVisitor = new SparqlExprVisitor( queryBuilder );
		SparqlSelectItemVisitor selectItemVisitor = new SparqlSelectItemVisitor( queryBuilder );
		

		// Process FROM to get table names loaded in the builder
		if (plainSelect.getFromItem() != null) {
			plainSelect.getFromItem().accept(fromVisitor);
		}
		
		if (plainSelect.getDistinct() != null) {
			queryBuilder.setDistinct();
			if (plainSelect.getDistinct().getOnSelectItems() != null) {
				throw new UnsupportedOperationException( "DISTINCT ON() is not supported");
			}
		}

		// process Joins to pick up new tables
		if (plainSelect.getJoins() != null) {
			for (Iterator iter = plainSelect.getJoins().iterator(); iter.hasNext();) {
				Join join = (Join) iter.next();
				deparseJoin( join );	
			}
		}

		// process the select -- All tables must be identified before this.
		for (Iterator iter = plainSelect.getSelectItems().iterator(); iter.hasNext();) {
			SelectItem selectItem = (SelectItem) iter.next();
			selectItem.accept( selectItemVisitor );
		}

		// process the where to add filters.
		if (plainSelect.getWhere() != null) {
			plainSelect.getWhere().accept(expressionVisitor);
			queryBuilder.addFilter( expressionVisitor.getResult() );
		}

		if (plainSelect.getGroupByColumnReferences() != null) {
			throw new UnsupportedOperationException( "GROUP BY is not supported");
//			buffer.append(" GROUP BY ");
//			for (Iterator iter = plainSelect.getGroupByColumnReferences().iterator(); iter.hasNext();) {
//				Expression columnReference = (Expression) iter.next();
//				columnReference.accept(expressionVisitor);
//				if (iter.hasNext()) {
//					buffer.append(", ");
//				}
//			}
		}

		if (plainSelect.getHaving() != null) {
			throw new UnsupportedOperationException( "HAVING is not supported");
//			buffer.append(" HAVING ");
//			plainSelect.getHaving().accept(expressionVisitor);
		}

		if (plainSelect.getOrderByElements() != null) {
			deparseOrderBy(plainSelect.getOrderByElements());
		}

		// TOP is implements in SPARQL as LIMIT
		
		Top top = plainSelect.getTop();
		Limit limit = plainSelect.getLimit();
		if (top != null && limit != null)
		{
			throw new IllegalStateException( "Top and Limit may not both be specified");
		}
		if (top != null)
		{
			if (top.isRowCountJdbcParameter())
			{
				throw new UnsupportedOperationException( "TOP with JDBC Parameter is not supported");
			}
			limit = new Limit();
			limit.setRowCount(top.getRowCount() );
		}

		if (limit != null) {
			deparseLimit(plainSelect.getLimit());
		}

	}

	private void deparseLimit(Limit limit) {
		// LIMIT n OFFSET skip 
		if (limit.isOffsetJdbcParameter()) {
			throw new UnsupportedOperationException( "LIMIT with OFFSET JDBC Parameter is not supported");
		} else if (limit.getOffset() != 0) {
			queryBuilder.setOffset( limit.getOffset() );
		}
		if (limit.isRowCountJdbcParameter()) {
			throw new UnsupportedOperationException( "LIMIT with JDBC Parameter is not supported");
			//buffer.append("?");
		} else if (limit.getRowCount() != 0) {
			queryBuilder.setLimit( limit.getRowCount() );
		} else {
			throw new UnsupportedOperationException( "LIMIT with no parameter is not supported");
		}
	}

	@Override
	public void visit( Union union )
	{
		throw new UnsupportedOperationException( "UNION is not supported");
	}
	
	private void deparseOrderBy(List orderByElements) {
		for (Iterator iter = orderByElements.iterator(); iter.hasNext();) {
			OrderByElement orderByElement = (OrderByElement) iter.next();
			orderByElement.accept(this);
		}
	}

	@Override
	public void visit( OrderByElement orderBy )
	{
		SparqlExprVisitor expressionVisitor = new SparqlExprVisitor( queryBuilder );
		orderBy.getExpression().accept(expressionVisitor);
		queryBuilder.addOrderBy( expressionVisitor.getResult(), orderBy.isAsc() );
		if (!expressionVisitor.isEmpty())
		{
			throw new IllegalStateException( "Order By processing failed -- stack not empty");
		}
	}
	
	/*	
	 * public void addJoin( Join join )
	{
		if (join.isSimple())
		{
			joinsInQuery.add(join);
			Object ri = join.getRightItem();
		}
		else
		{
			throw new UnsupportedOperationException( "JOIN type is not supported");
		}
	}
	
	*/
	private void deparseJoin(Join join) {
		if (join.isSimple())
		{
			SparqlFromVisitor fromVisitor = new SparqlFromVisitor( queryBuilder );
			join.getRightItem().accept(fromVisitor);
		}
		else
		{
			String fmt = "%s %s JOIN Is not supported";
			String inOut = join.isOuter()?"OUTER":"INNER";
			if (join.isRight())
				throw new UnsupportedOperationException( String.format( fmt, "RIGHT", inOut));
			else if (join.isNatural())
				throw new UnsupportedOperationException( String.format( fmt, "NATURAL", inOut));
			else if (join.isFull())
				throw new UnsupportedOperationException( String.format( fmt, "FULL", inOut));
			else if (join.isLeft())
				throw new UnsupportedOperationException( String.format( fmt, "LEFT", inOut));
		}
		/*
		FromItem fromItem = join.getRightItem();
		fromItem.accept(this);
		
		
		if (join.getOnExpression() != null) {
			buffer.append(" ON ");
			join.getOnExpression().accept(expressionVisitor);
		}
		if (join.getUsingColumns() != null) {
			buffer.append(" USING ( ");
			for (Iterator iterator = join.getUsingColumns().iterator(); iterator.hasNext();) {
				Column column = (Column) iterator.next();
				buffer.append(column.getWholeColumnName());
				if (iterator.hasNext()) {
					buffer.append(" ,");
				}
			}
			buffer.append(")");
		}
		*/
	}
	
}
