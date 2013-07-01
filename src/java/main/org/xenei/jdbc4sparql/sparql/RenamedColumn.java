package org.xenei.jdbc4sparql.sparql;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;

public class RenamedColumn implements Column
{
	public String name;
	public Column baseColumn;

	public RenamedColumn( final String name, final Column baseColumn )
	{
		this.name = name;
		this.baseColumn = baseColumn;
	}

	@Override
	public Catalog getCatalog()
	{
		return baseColumn.getCatalog();
	}

	@Override
	public ColumnDef getColumnDef()
	{
		return baseColumn.getColumnDef();
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public Schema getSchema()
	{
		return baseColumn.getSchema();
	}

	@Override
	public String getSPARQLName()
	{
		return baseColumn.getSPARQLName();
	}

	@Override
	public String getSQLName()
	{
		return baseColumn.getSQLName();
	}

	@Override
	public Table getTable()
	{
		return baseColumn.getTable();
	}

}