package org.xenei.jdbc4sparql.sparql.visitors;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

class SparqlFromVisitor implements FromItemVisitor
{
	SparqlQueryBuilder builder;

	SparqlFromVisitor( final SparqlQueryBuilder builder )
	{
		this.builder = builder;
	}

	@Override
	public void visit( final SubJoin subjoin )
	{
		throw new UnsupportedOperationException("FROM subjoin is not supported");
	}

	@Override
	public void visit( final SubSelect subSelect )
	{
		throw new UnsupportedOperationException(
				"FROM subselect is not supported");
	}

	@Override
	public void visit( final Table tableName )
	{
		builder.addTable(tableName.getSchemaName(), tableName.getName());
		if (builder.isAllColumns())
		{
			builder.addVars(builder.getCatalog()
					.getSchema(tableName.getSchemaName())
					.getTable(tableName.getName()).getColumns());
		}
	}

}