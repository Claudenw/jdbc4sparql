package org.xenei.jdbc4sparql.sparql.visitors;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;


import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
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
		
		if (plainSelect.getDistinct() != null) {
			queryBuilder.setDistinct();
			if (plainSelect.getDistinct().getOnSelectItems() != null) {
				throw new UnsupportedOperationException( "DISTINCT ON() is not supported");
			}
		}
		
		for (Iterator iter = plainSelect.getSelectItems().iterator(); iter.hasNext();) {
			SelectItem selectItem = (SelectItem) iter.next();
			selectItem.accept( selectItemVisitor );
		}

		
		if (plainSelect.getFromItem() != null) {
			plainSelect.getFromItem().accept(fromVisitor);
		}

		if (plainSelect.getJoins() != null) {
			throw new UnsupportedOperationException( "JOIN is not supported");
//			for (Iterator iter = plainSelect.getJoins().iterator(); iter.hasNext();) {
//				Join join = (Join) iter.next();
//				deparseJoin(join);		
//			}
		}

		if (plainSelect.getWhere() != null) {
			plainSelect.getWhere().accept(expressionVisitor);
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

	public void deparseLimit(Limit limit) {
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
	
	public void deparseOrderBy(List orderByElements) {
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
}
