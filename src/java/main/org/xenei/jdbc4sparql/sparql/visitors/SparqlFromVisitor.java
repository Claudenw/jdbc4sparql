package org.xenei.jdbc4sparql.sparql.visitors;

import java.sql.SQLException;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

class SparqlFromVisitor implements FromItemVisitor
{
	private SparqlQueryBuilder builder;
	
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
		try
		{
			builder.addTable(tableName.getSchemaName(), tableName.getName());
		}
		catch (SQLException e)
		{
			throw new RuntimeException( e );
		}
	}

}