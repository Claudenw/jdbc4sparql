package org.xenei.jdbc4sparql.sparql.visitors;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;


class SparqlSelectItemVisitor implements SelectItemVisitor
{
	
	private SparqlQueryBuilder queryBuilder;
	
	SparqlSelectItemVisitor(SparqlQueryBuilder queryBuilder)
	{
		this.queryBuilder = queryBuilder;
	}
	
	@Override
	public void visit( AllColumns allColumns )
	{
		queryBuilder.setAllColumns();
	}

	@Override
	public void visit( AllTableColumns allTableColumns )
	{
		// FIXME this should insert variables into the variable list and 
		// insert triples into the select ot get them selected.
		// buffer.append(allTableColumns.getTable().getWholeTableName() + ".*");
		throw new UnsupportedOperationException( "AllTableColumns is not supported");
	}

	@Override
	public void visit( SelectExpressionItem selectExpressionItem )
	{
		StringBuilder buffer = new StringBuilder();
		if (selectExpressionItem.getAlias() != null) {
			buffer.append( "(");
		}
		
		SparqlExprVisitor v = new SparqlExprVisitor( queryBuilder );
		selectExpressionItem.getExpression().accept( v);
		buffer.append( v.getResult());
		if (selectExpressionItem.getAlias() != null) {
			buffer.append(" AS " ).append( selectExpressionItem.getAlias()).append(")");
		}
		queryBuilder.addVar( buffer.toString());
	}

	
	
}