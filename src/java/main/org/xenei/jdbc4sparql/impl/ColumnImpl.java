package org.xenei.jdbc4sparql.impl;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;

public class ColumnImpl extends NamespaceImpl implements Column
{
	private Table table;
	private ColumnDef columnDef;
	
	public ColumnImpl( Table table, ColumnDef columnDef)
	{
		this( table.getNamespace(), table, columnDef );
	}
	
	public ColumnImpl( String namespace, Table table, ColumnDef columnDef )
	{
		super( namespace, columnDef.getLabel());
		this.table = table;
		this.columnDef = columnDef;
	}
	public String getColumnClassName()
	{
		return columnDef.getColumnClassName();
	}
	public int getDisplaySize()
	{
		return columnDef.getDisplaySize();
	}
	public String getLabel()
	{
		return columnDef.getLabel();
	}
	public int getType()
	{
		return columnDef.getType();
	}
	public String getTypeName()
	{
		return columnDef.getTypeName();
	}
	public int getPrecision()
	{
		return columnDef.getPrecision();
	}
	public int getScale()
	{
		return columnDef.getScale();
	}
	public boolean isAutoIncrement()
	{
		return columnDef.isAutoIncrement();
	}
	public boolean isCaseSensitive()
	{
		return columnDef.isCaseSensitive();
	}
	public boolean isCurrency()
	{
		return columnDef.isCurrency();
	}
	public boolean isDefinitelyWritable()
	{
		return columnDef.isDefinitelyWritable();
	}
	public int getNullable()
	{
		return columnDef.getNullable();
	}
	public boolean isReadOnly()
	{
		return columnDef.isReadOnly();
	}
	public boolean isSearchable()
	{
		return columnDef.isSearchable();
	}
	public boolean isSigned()
	{
		return columnDef.isSigned();
	}
	public boolean isWritable()
	{
		return columnDef.isWritable();
	}
	
	@Override
	public Catalog getCatalog()
	{
		return getSchema().getCatalog();
	}
	@Override
	public Schema getSchema()
	{
		return table.getSchema();
	}
	@Override
	public Table getTable()
	{
		return table;
	}

}
