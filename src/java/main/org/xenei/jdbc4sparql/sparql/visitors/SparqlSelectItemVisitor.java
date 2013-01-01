package org.xenei.jdbc4sparql.sparql.visitors;

import com.hp.hpl.jena.sparql.expr.Expr;

import java.sql.SQLException;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

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
		try {
			queryBuilder.setAllColumns();
		}
		catch (SQLException e)
		{
			throw new RuntimeException( e );
		}
	}

	@Override
	public void visit( AllTableColumns allTableColumns )
	{
		Table tbl = null;
	
			for (Schema s : queryBuilder.getCatalog().findSchemas(allTableColumns.getTable().getSchemaName()))
			{
				for (Table t : s.findTables(allTableColumns.getTable().getName()))
				{
					if (tbl == null)
					{
						tbl = t;
					}
					else
					{
						throw new IllegalStateException( "Duplicate table names "+allTableColumns.getTable().getWholeTableName());
					}
				}
			}
			if (tbl == null)
			{
				throw new IllegalStateException( "Table "+allTableColumns.getTable().getWholeTableName()+" not found");
			}
			for (Column c : tbl.findColumns(null))
			{
				try
				{
					queryBuilder.addVar(c, null );
				}
				catch (SQLException e)
				{
					throw new RuntimeException( e );
				}
			}

	}

	@Override
	public void visit( SelectExpressionItem selectExpressionItem )
	{
		
		SparqlExprVisitor v = new SparqlExprVisitor( queryBuilder );
		selectExpressionItem.getExpression().accept( v );
		// handle explicit name mapping
		if (selectExpressionItem.getAlias() != null)
		{
			queryBuilder.addVar( v.getResult(), selectExpressionItem.getAlias() );
		}
		else
		{
			// handle implicit name mapping
			if (selectExpressionItem.getExpression() instanceof net.sf.jsqlparser.schema.Column)
			{
				queryBuilder.addVar( v.getResult(), selectExpressionItem.getExpression().toString() );
			}
			else
			{
				// handle no name mapping
				queryBuilder.addVar( v.getResult(), null );
			}
		}
			
	}

	
	
}