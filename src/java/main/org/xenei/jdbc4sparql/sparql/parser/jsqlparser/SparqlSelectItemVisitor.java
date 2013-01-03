package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.sql.SQLException;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.sparql.SparqlColumn;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlTable;

class SparqlSelectItemVisitor implements SelectItemVisitor
{

	private final SparqlQueryBuilder queryBuilder;

	SparqlSelectItemVisitor( final SparqlQueryBuilder queryBuilder )
	{
		this.queryBuilder = queryBuilder;
	}

	@Override
	public void visit( final AllColumns allColumns )
	{
		try
		{
			queryBuilder.setAllColumns();
		}
		catch (final SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public void visit( final AllTableColumns allTableColumns )
	{
		SparqlTable tbl = null;

		for (final Schema s : queryBuilder.getCatalog().findSchemas(
				allTableColumns.getTable().getSchemaName()))
		{
			for (final Table t : s.findTables(allTableColumns.getTable()
					.getName()))
			{
				if (t instanceof SparqlTable)
				{
					if (tbl == null)
					{
						tbl = (SparqlTable) t;
					}
					else
					{
						throw new IllegalStateException(
								"Duplicate table names "
										+ allTableColumns.getTable()
												.getWholeTableName());
					}
				}
			}
		}
		if (tbl == null)
		{
			throw new IllegalStateException("Table "
					+ allTableColumns.getTable().getWholeTableName()
					+ " not found");
		}
		for (final Column c : tbl.findColumns(null))
		{
			try
			{
				queryBuilder.addVar((SparqlColumn) c, null);
			}
			catch (final SQLException e)
			{
				throw new RuntimeException(e);
			}
		}

	}

	@Override
	public void visit( final SelectExpressionItem selectExpressionItem )
	{

		final SparqlExprVisitor v = new SparqlExprVisitor(queryBuilder);
		selectExpressionItem.getExpression().accept(v);
		// handle explicit name mapping
		if (selectExpressionItem.getAlias() != null)
		{
			queryBuilder.addVar(v.getResult(), selectExpressionItem.getAlias());
		}
		else
		{
			// handle implicit name mapping
			if (selectExpressionItem.getExpression() instanceof net.sf.jsqlparser.schema.Column)
			{
				queryBuilder.addVar(v.getResult(), selectExpressionItem
						.getExpression().toString());
			}
			else
			{
				// handle no name mapping
				queryBuilder.addVar(v.getResult(), null);
			}
		}

	}

}